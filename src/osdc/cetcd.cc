#include "cetcd.h"
#include "cetcd_json_parser.h"

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <libgen.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <curl/curl.h>
#include <sys/select.h>
#include <pthread.h>
#include "ObjectCacher.h"

/*** cetcd:cetcd_client ***/

#define dout_subsys ceph_subsys_objectcacher
#undef dout_prefix
#define dout_prefix *_dout << "cetcd:cetcd_client" << __FILE__ << ":" << __func__

typedef struct cetcd_response_parser_t {
    int st;
    int http_status;
    enum ETCD_API_TYPE api_type;
    cetcd_string buf;
    void *resp;
    yajl_parser_context ctx;
    yajl_handle json;
}cetcd_response_parser;


static const char *http_method[] = {
    "GET",
    "POST",
    "PUT",
    "DELETE",
    "HEAD",
    "OPTION"
};

static const char *cetcd_event_action[] = {
    "set",
    "get",
    "update",
    "create",
    "delete",
    "expire",
    "compareAndSwap",
    "compareAndDelete"
};

cetcd_client* cetcd_client_create(ObjectCacher *oc, const std::string &servers)
{
	cetcd_client *cli = NULL;
	cetcd_array config_addrs;
	if (servers.empty() || NULL == oc) {
		return NULL;
	}

	ldout(oc->cct, 11) << " config server: " << servers << dendl;
	cetcd_array_init(&config_addrs, 1);

	//parse server
	std::string delim = ",";
	std::string prefix = "http://";
	std::string ip_port;
	
	char* p = strtok((char*)servers.c_str(), delim.c_str());
	ip_port = prefix + p;
	cetcd_array_append(&config_addrs, (char*)ip_port.c_str());
	while ((p = strtok(NULL, delim.c_str()))) {
		ip_port = prefix + p;
		cetcd_array_append(&config_addrs, (char*)ip_port.c_str());
	}

    cli = new cetcd_client(oc);
    cli->cetcd_client_init(&config_addrs);
	
	cetcd_array_destroy(&config_addrs);
	return cli;
}

void cetcd_client_release(cetcd_client *cli)
{
	if (cli) {
    	cli->cetcd_client_destroy();
		delete cli;
	}
}

void cetcd_client::cetcd_client_init(cetcd_array *address) 
{
    size_t i;
    cetcd_array *addrs;
    cetcd_string addr;
    curl_global_init(CURL_GLOBAL_ALL);
    srand(time(0));

    keys_space =   "v2/keys";
    stat_space =   "v2/stat";
    member_space = "v2/members";
    curl = curl_easy_init();

    addrs = cetcd_array_create(cetcd_array_size(address));
    for (i=0; i<cetcd_array_size(address); ++i) {
        addr = (cetcd_string)cetcd_array_get(address, i);
        if ( strncmp(addr, "http", 4)) {
            cetcd_array_append(addrs, sdscatprintf(sdsempty(), "http://%s", addr));
        } else {
            cetcd_array_append(addrs, sdsnew(addr));
        }
    }

    addresses = cetcd_array_shuffle(addrs);
    picked = rand() % (cetcd_array_size(addresses));

    settings.verbose = 0;
    settings.connect_timeout = 1;
    settings.read_timeout = 1;  /*not used now*/
    settings.write_timeout = 1; /*not used now*/
    settings.user = NULL;
    settings.password = NULL;

    cetcd_array_init(&watchers, 10);

    /* Set CURLOPT_NOSIGNAL to 1 to work around the libcurl bug:
     *  http://stackoverflow.com/questions/9191668/error-longjmp-causes-uninitialized-stack-frame
     *  http://curl.haxx.se/mail/lib-2008-09/0197.html
     * */
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

#if LIBCURL_VERSION_NUM >= 0x071900
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 1L); /*the same as go-etcd*/
#endif
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "cetcd");
    curl_easy_setopt(curl, CURLOPT_POSTREDIR, 3L);     /*post after redirecting*/
    curl_easy_setopt(curl, CURLOPT_SSLVERSION, CURL_SSLVERSION_TLSv1);
}

void cetcd_client::cetcd_client_destroy() 
{
    cetcd_addresses_release(addresses);
    cetcd_array_release(addresses);
    sdsfree(settings.user);
    sdsfree(settings.password);
    curl_easy_cleanup(curl);
    curl_global_cleanup();
    cetcd_array_destroy(&watchers);
}

void cetcd_client::cetcd_addresses_release(cetcd_array *addrs)
{
    int count, i;
    cetcd_string s;
    if (addrs) {
        count = cetcd_array_size(addrs);
        for (i = 0; i < count; ++i) {
            s = (cetcd_string)cetcd_array_get(addrs, i);
            sdsfree(s);
        }
    }
}

void cetcd_client::cetcd_client_sync_cluster()
{
    cetcd_request req;
    cetcd_array *addrs;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_GET;
    req.api_type = ETCD_MEMBERS;
    req.uri = sdscatprintf(sdsempty(), "%s", member_space);
    addrs =  (cetcd_array*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    if (addrs == NULL) {
        return ;
    }
    cetcd_addresses_release(addresses);
    cetcd_array_release(addresses);
    addresses = cetcd_array_shuffle(addrs);
    picked = rand() % (cetcd_array_size(addresses));
}

void cetcd_client::cetcd_setup_user(const char* user, const char* password)
{
    if (user != NULL) {
        settings.user = sdsnew(user);
    }
    if (password !=NULL) {
        settings.password = sdsnew(password);
    }
}

void cetcd_client::cetcd_setup_tls(const char *CA, const char *cert, const char *key) 
{
    if (CA) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, CA);
    }
    if (cert) {
        curl_easy_setopt(curl, CURLOPT_SSLCERT, cert);
    }
    if (key) {
        curl_easy_setopt(curl, CURLOPT_SSLKEY, key);
    }
}

size_t cetcd_parse_response(char *ptr, size_t size, size_t nmemb, void *userdata);

cetcd_watcher *cetcd_client::cetcd_watcher_create(const char *key, uint64_t index,
        int recursive, int once, cetcd_watcher_callback callback, void *userdata) 
{
    cetcd_watcher *watcher;

    watcher = (cetcd_watcher*)calloc(1, sizeof(cetcd_watcher));
    watcher->cli = this;
    watcher->key = sdsnew(key);
    watcher->index = index;
    watcher->recursive = recursive;
    watcher->once = once;
    watcher->callback = callback;
    watcher->userdata = userdata;
    watcher->curl = curl_easy_init();

    watcher->parser = (cetcd_response_parser*)calloc(1, sizeof(cetcd_response_parser));
    watcher->parser->st = 0;
    watcher->parser->buf = sdsempty();
    watcher->parser->resp = (cetcd_response*)calloc(1, sizeof(cetcd_response));

    watcher->array_index = -1;

    return watcher;
}

void cetcd_client::cetcd_watcher_release(cetcd_watcher *watcher) 
{
    if (watcher) {
        if (watcher->key) {
            sdsfree(watcher->key);
        }
        if (watcher->curl) {
            curl_easy_cleanup(watcher->curl);
        }
        if (watcher->parser) {
            sdsfree(watcher->parser->buf);
            if (watcher->parser->json) {
                yajl_free(watcher->parser->json);
                cetcd_array_destroy(&watcher->parser->ctx.keystack);
                cetcd_array_destroy(&watcher->parser->ctx.nodestack);
            }
            cetcd_response_release((cetcd_response*)watcher->parser->resp);
            free(watcher->parser);
        }
        free(watcher);
    }
}

/*reset the temp resource one time watching used*/
void cetcd_client::cetcd_watcher_reset(cetcd_watcher *watcher) 
{
    if (!watcher){
        return;
    }

    /*reset the curl handler*/
    curl_easy_reset(watcher->curl);
    cetcd_curl_setopt(watcher->curl, watcher);

    if (watcher->parser) {
        watcher->parser->st = 0;
        /*allocate the resp, because it is freed after calling the callback*/
        watcher->parser->resp = calloc(1, sizeof(cetcd_response));

        /*clear the buf, it is allocated by cetcd_watcher_create,
         * so should only be freed in cetcd_watcher_release*/
        sdsclear(watcher->parser->buf);

        /*the json object created by cetcd_parse_response, so it should be freed
         * after having got some response*/
        if (watcher->parser->json) {
            yajl_free(watcher->parser->json);
            cetcd_array_destroy(&watcher->parser->ctx.keystack);
            cetcd_array_destroy(&watcher->parser->ctx.nodestack);
            watcher->parser->json = NULL;
        }
    }
}

cetcd_string cetcd_client::cetcd_watcher_build_url(cetcd_watcher *watcher) 
{
    cetcd_string url;
    url = sdscatprintf(sdsempty(), "%s/%s%s?wait=true", (cetcd_string)cetcd_array_get(addresses, picked),
            keys_space, watcher->key);
    if (watcher->index) {
        url = sdscatprintf(url, "&waitIndex=%lu", watcher->index);
    }
    if (watcher->recursive) {
        url = sdscatprintf(url, "&recursive=true");
    }
    return url;
}

bool cetcd_client::cetcd_check_mount_stat()
{
	bool mounted = false;
	char p_dir[256] = {0};
	struct stat st, p_st;
	
	int s = cache_path.find_last_of('/');
	strncpy(p_dir, (char*)cache_path.c_str(), s+1);
	
	if (stat(cache_path.c_str(), &st) != 0) {
		lderr(oc->cct) << "Failed to get stat info for  " << cache_path.c_str()
			<< ", err msg: " << cpp_strerror(errno) << dendl;
		return mounted;
	}
	if (stat(p_dir, &p_st) != 0) {
		lderr(oc->cct) << "Failed to get stat info for " << p_dir 
			<< ", err msg: " << cpp_strerror(errno) << dendl;
		return mounted;
	}

	if (st.st_dev != p_st.st_dev)
		mounted = true;

	return mounted;
}

int cetcd_client::cetcd_get_ips(vector <std::string> &ips)
{
	int r;
	char localhost[64] = {0};
	struct addrinfo hint;
    struct addrinfo *res, *ores;
	
	r = gethostname(localhost, sizeof(localhost));
	if (r == -1) {
		lderr(oc->cct) << "Failed to get host name"
			<< ", err msg: " << cpp_strerror(errno) << dendl;
		return r;
	}
	ldout(oc->cct, 11) << " host name: " << localhost << dendl;
	
	//get <ip hostname> map from /etc/hosts
	memset(&hint, 0, sizeof(hint));
    hint.ai_family = AF_UNSPEC;
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_protocol = IPPROTO_TCP;

	r = getaddrinfo(localhost, NULL, &hint, &res);
	if (r < 0) {
	    lderr(oc->cct) << " Failed to find host: " << localhost 
			<< ", err msg: " << gai_strerror(r) << dendl;
		return r;
	}

	ores = res;
	while (res) {
		char ipstr[16] = {0};
		inet_ntop(AF_INET, &(((struct sockaddr_in *)(res->ai_addr))->sin_addr), ipstr, sizeof(ipstr));

		ips.push_back((char*)ipstr);
		res = res->ai_next;
	}
	freeaddrinfo(ores);

	return 0;
}

int cetcd_client::cetcd_check_role(char *uuidstring, int len)
{
	int r;
	vector<std::string> ips;

	if (NULL == uuidstring || len <= 0)
		return -EINVAL;
	
	r = cetcd_get_ips(ips);
	if (r != 0)
		return r;

	vector<std::string>::iterator it = ips.begin();
	while (it != ips.end()) {
		dev_map_iterator it1 = server_dev_map.find(*it);
		role_map_iterator it2 = server_role_map.find(*it);
		if (it1 != server_dev_map.end()) {
			strncpy(uuidstring, (it1->second).c_str(), 
				(it1->second.length() > (unsigned int)len ? len: it1->second.length()));
			return it2->second;
		}
		++it;
	}

	return ETCD_OTHER;
}

int cetcd_client::cetcd_attach_device(char *devname, int size)
{
	int r;
	char uuidstring[64] = {0};

	if (NULL == devname || size <= 0)
		return -EINVAL;

	int role = cetcd_check_role((char*)uuidstring, sizeof(uuidstring));
	switch( role ) {
	case ETCD_SLAVE: 
		{
			ldout(oc->cct, 5) << " Running on slave node, Cache is degraded?"
							  << " for your data safty, please rebuild cache immediately" << dendl;
			/*running on slave node,
			these could happen on the following two cases:
			 1. master node crashed, migrate to slave node
			 2. scheduled migration, migrate from master node to slave node 


			 CAUTION: to simplify, we assue the master node has dead in the following 
			 code implementation, for the second case, you should deal with the master 
			 data copy manually(ie, assemble raid1 device on the slave node).
			*/
			//systemctl stop target.service
			ldout(oc->cct, 11) << " Try to stop service target.service" << dendl;
			r = cetcd_stop_target();
			if (r < 0)
				goto failed;

			//convert UUID to name
			ldout(oc->cct, 11) << " Try to convert uuid " 
							   << uuidstring << " to device name " << dendl;
			r = cetcd_convert_device(uuidstring, devname, size);
			if (r < 0) 
				goto failed;

			//mdadm -S /dev/md{X}
			ldout(oc->cct, 11) << " Try to stop inactive mdraid with"
							   << " device= " << devname << dendl;
			r = cetcd_stop_inactive_raid(devname);
			if (r < 0) 
				goto failed;
			
			//mdadm --assemble /dev/md{X} /dev/nvme0n1 --run? 
			ldout(oc->cct, 11) << " Try to assemble mdraid with"
							   << " device= " << devname << dendl;
			r = cetcd_assemble_raid(devname);
			if (r < 0) 
				goto failed;	
		}
		break;
	case ETCD_MASTER: //normal case, running on master node
		/*administrator ensure cache device has been prepared well 
		on the installation and config phase, default raid device: /dev/md0*/
		r = 0;
		break;
	case ETCD_OTHER: 
		{
			ldout(oc->cct, 5) << " Running on the third node, Cache is degraded?"
							  << " for your data safty, please rebuild cache immediately" << dendl;
			/* running on the third available node, 
			these could happened on the following two cases:
			  1. scheduled migration: master/slave node -> the third node
			  2. disaster recovery migration: master node crashed, migrate to the third node 

			  CAUTION: to simplify, we only use the first active node(device) as our target
			  (ie, cache data reside on a degrade raid device , for your data safty, 
			  you should promote your cache device as soon as possible)
			*/
			//ping {tip} -c 4
			ldout(oc->cct, 11) << " Try to get target ip" << dendl;
			char tip[32] = {0};
			r = cetcd_get_target_ip(tip, sizeof(tip));
			if (r < 0)
				goto failed;

			//iscsiadm -m discovery -t st -p {target-ip}
			ldout(oc->cct, 11) << " Try to discovery target from portal: " << tip << dendl;
			char iqn[64] = {0};
			r = cetcd_discovery_target(tip, iqn, sizeof(iqn));
			if (r < 0)
			 	goto failed;
			 
			//iscsiadm -m node -T {target-iqn} -p {target-ip} -l
			ldout(oc->cct, 11) << " Try to login target: " << iqn 
							   << " on portal: " << tip << dendl;
			r = cetcd_login_target(tip, iqn);
			if (r < 0)
			 	goto failed;
			 
			//iscsiadm -m session -P 3
			ldout(oc->cct, 11) << " Try to get iscsi device" << dendl;
			r = cetcd_get_device(iqn, devname, size);
			if (r < 0)
			 	goto failed;

			//mdadm -S /dev/md{X}
			ldout(oc->cct, 11) << " Try to stop inactive mdraid with"
			                   << " device= " << devname << dendl;
			r = cetcd_stop_inactive_raid(devname);
			if (r < 0)
				goto failed;
			 
			//mdadm --assemble /dev/md{X} /dev/{dev} --run?
			ldout(oc->cct, 11) << " Try to assemble mdraid with"
			                   << " deivce= " << devname << dendl;
			r = cetcd_assemble_raid(devname);
			if (r < 0)
			 	goto failed;
		}
		break;
	default:
		r = role;
		goto failed;
	}

failed:
	if (role == ETCD_SLAVE)
		cetcd_start_target();
	return r;
}

int cetcd_client::cetcd_mount_device(const char *dev, const char *path)
{
	//mount {dev} {path} 2>&1
	if (NULL == dev || NULL == path)
		return -EINVAL;

	bool succeed = true;
	FILE *pf = NULL;
	char cmd[128] = {0};
	snprintf(cmd, sizeof(cmd), "mount %s %s 2>&1", dev, path);

	pf = popen(cmd, "r");
	if (NULL == pf) {
		int r = errno;
		lderr(oc->cct) << " Failed to create sub process: " << cmd
			           << " err msg: " << cpp_strerror(r) << dendl;
		return -r;
	}
	ldout(oc->cct, 11) << " Succeed to create sub process: " << cmd << dendl;

	char key[256] = {0};
	snprintf(key, sizeof(key), "%s is alreadly mounted on %s", dev, path);
	char output[1024] = {0};
	while(fgets(output, sizeof(output), pf) != NULL) {
		if (strstr(output, key) == NULL) {
			succeed = false;
			lderr(oc->cct) << "Failed to mount " << dev << " to " << path
				<< ", err msg: " << output << dendl;
		} else {
			succeed = true;
		}
	}
	
	pclose(pf);
	return (succeed? 0: -1);
}

int cetcd_client::cetcd_start_target()
{
	bool succeed = true;
	FILE *pf = NULL;
	const char *cmd = "systemctl start target.service";
	
	pf = popen(cmd, "r");
	if (NULL == pf) {
		int r = errno;
		lderr(oc->cct) << " Failed to create sub process: " << cmd 
			           << " err msg: " << cpp_strerror(r) << dendl;
		return -r;
	}
	ldout(oc->cct, 11) << " Succeed to create sub process: " << cmd << dendl;

	char output[1024] = {0};
	while(fgets(output, sizeof(output), pf) != NULL) {
		succeed = false;
		lderr(oc->cct) << " Failed to start target.service, err msg: " << output << dendl;
	}
	
	pclose(pf);
	return (succeed? 1: -1);
}

int cetcd_client::cetcd_stop_target()
{
	bool succeed = true;
	FILE *pf = NULL;
	const char *cmd = "systemctl stop target.service";

	pf = popen(cmd, "r");
	if (NULL == pf) {
		int r = errno;
		lderr(oc->cct) << " Failed to create sub process: " << cmd 
			           << " err msg: " << cpp_strerror(r) << dendl;
		return -r;
	}
	ldout(oc->cct, 11) << " Succeed to create sub process: " << cmd << dendl;

	char output[1024] = {0};
	while(fgets(output, sizeof(output), pf) != NULL) {
		succeed = false;
		lderr(oc->cct) << "Failed to stop target.service"
			<<", err msg: " << output << dendl;
	}

	pclose(pf);
	return (succeed? 0: -1);
}

int cetcd_client::cetcd_stop_inactive_raid(const char *devname)
{
	FILE *pf = NULL;
	char cmd[128] = {0};

	// /dev/sdn
	char *tmp_dev = strrchr((char*)devname, '/');
	++tmp_dev;
	snprintf(cmd, sizeof(cmd), "cat /proc/mdstat|grep -e \"inactive %s\"", tmp_dev);
	pf = popen(cmd, "r");
	if (NULL == pf) {
		int r = errno;
		lderr(oc->cct) << " Failed to create sub process: " << cmd 
			           << " err msg: " << cpp_strerror(r) << dendl;
		return -r;
	}
	ldout(oc->cct, 11) << " Succeed to create sub process: " << cmd << dendl;

	/*cat /proc/mdstat 

	Personalities : [raid1] 
	md127 : inactive sdn[1](S)
      		1953382400 blocks super 1.2
	*/
	char output[512] = {0};
	if(fgets(output, sizeof(output), pf) != NULL) {
		char *raid = strtok(output, ":");
		assert(raid);
		
		//stop inactive raid
		snprintf(cmd, sizeof(cmd), "mdadm -S /dev/%s", raid);
		FILE *pf1 = popen(cmd, "r");
		assert(pf1);
		pclose(pf1);
	}

	pclose(pf);
	return 0;
}

int cetcd_client::cetcd_assemble_raid(const char *devname)
{
	if (NULL == devname)
		return -EINVAL;

	int num = 0;
	bool succeed = false;
	while (num < 128 && !succeed) {
		FILE *pf = NULL;
		char cmd[128] = {0};
		char output[512] = {0};
		
		snprintf(cmd, sizeof(cmd), "mdadm --assemble /dev/md%d %s --run 2>&1", num, devname);
		pf = popen(cmd, "r");
		if (NULL == pf) {
			int r = errno;
			lderr(oc->cct) << " Failed to create sub process: " << cmd 
				           << " err msg: " << cpp_strerror(r) << dendl;
			return -r;
		}
		ldout(oc->cct, 11) << " Succeed to create sub process: " << cmd << dendl;

		/*mdadm --assemble /dev/md{0|X} /dev/nvme0n1 --run? 

		Possible output:
	 		mdadm: /dev/md0 is already in use? 
	 		mdadm: /dev/md1 has been started with 1 drive (out of 2)?
	 		mdadm: /dev/sdn is busy - skipping  - pls check /proc/mdstat for details
		*/
		const char *key1 = "has been started with";
		const char *key2 = "is already in use";
		if (fgets(output, sizeof(output), pf) != NULL) {
			if (strstr(output, key1) != NULL) {
				succeed = true;
				ldout(oc->cct, 11) << "Succeed to assemble raid /dev/md" << num
					<< " with device " << devname << dendl;
			} 
			else if (strstr(output, key2) != NULL) {
				lderr(oc->cct) << " Failed to assemble raid /dev/md" << num 
					<< " with device " << devname
					<< ", err msg: " << output << dendl;
			} else {
				lderr(oc->cct) << " Failed to assemble raid /dev/md" << num 
					<< " with device " << devname
					<< ", err msg: " << output << dendl;
				break;
			}
		}
		++num;
		pclose(pf);
	}

	return (succeed? (num - 1): -1);
}

int cetcd_client::cetcd_get_target_ip(char *tip, int size)
{
	/*ping ip -c 4
	 *4 packets transmitted, 0 received, +4 errors, 100% packet loss, time 2999ms
     *4 packets transmitted, 4 received, 0% packet loss, time 2999ms
	*/
	if (NULL == tip || size <= 0)
		return -EINVAL;

	//TODO: we now take the first active target as our candidate
	dev_map_iterator it, it_end = server_dev_map.end();
	for (it = server_dev_map.begin(); it != it_end; ++it) {
		FILE *pf = NULL;
		char cmd[128] = {0};
		snprintf(cmd, sizeof(cmd), "ping %s -c 4 | grep -e \"4 packets transmitted, 4 received\"", 
			(it->first).c_str());

		pf = popen(cmd, "r");
		if (NULL == pf) {
			int r = errno;
			lderr(oc->cct) << " Failed to create sub-process: " << cmd 
						   << " err msg: " << cpp_strerror(r) << dendl;
			return -r;
		}
		ldout(oc->cct, 11) << " Succeed to create sub-process: " << cmd << dendl;

		char output[1024] = {0};
		if (fgets(output, sizeof(output), pf) != NULL) {
			strncpy(tip, (it->first).c_str(), size);
			
			pclose(pf);
			ldout(oc->cct, 11) << " Succeed to find a target ip: " << tip << dendl;
			return 0;
		}
		pclose(pf);
		lderr(oc->cct) << "Failed to get target ip, err msg: " << output << dendl;
	}	
	
	lderr(oc->cct) << "Failed to get target ip" << dendl;
	return -1;
}

int cetcd_client::cetcd_discovery_target(const char *tip, char *iqn, int size)
{
	/*iscsiadm -m discovery -t st -p {target-ip}
	 *{target-ip:3260},{target-id} {iqn}
	 *10.10.5.23:3260,1 iqn.2016-12.com.ceph03:disk1
	*/
	if (NULL == tip ||
		NULL == iqn || size <= 0)
		return -EINVAL;

	FILE *pf = NULL;
	char cmd[128] = {0};
	snprintf(cmd, sizeof(cmd), "iscsiadm -m discovery -t st -p %s 2>&1", tip);
	pf = popen(cmd, "r");
	if (NULL == pf) {
		int r = errno;
		lderr(oc->cct) << " Failed to create sub-process: " << cmd
					   << " err msg: " << cpp_strerror(r) << dendl;
		return -r;
	}
	ldout(oc->cct, 11) << " Succeed to create sub process: " << cmd << dendl;

	char output[1024] = {0};
	char key[32] = {0};
	snprintf(key, sizeof(key), "%s:3260", tip);
	while (fgets(output, sizeof(output), pf) != NULL) {
		//TODO: we now take the first as our candidate 
		ldout(oc->cct, 10) << " output: " << output << dendl;

		if (strstr(output, key) != NULL) {
			char* target = strtok(output, " ");
			target = strtok(NULL, " ");
			assert(target);

			target[strlen(target)-1] = '\0'; //strip '/n'
			snprintf(iqn, size, "%s", target);

			pclose(pf);
			ldout(oc->cct, 11) << " Succeed to find a target iqn: " << iqn << dendl;
			return 0;
		}
		lderr(oc->cct) << " Failed to find target, err msg: " << output << dendl;
	}

	pclose(pf);
	lderr(oc->cct) << " Faild to discovery target from: " << tip << dendl;
	return -1;
}

int cetcd_client::cetcd_login_target(const char *tip, const char *iqn)
{
	if (NULL == tip || NULL == iqn)
		return -EINVAL;

	FILE *pf = NULL;
	char cmd[256] = {0};
	char output[1024] = {0};
	
	snprintf(cmd, sizeof(cmd), "iscsiadm -m node -T %s -p %s --login 2>&1", iqn, tip);
	pf = popen(cmd, "r");
	if (NULL == pf) {
		int r = errno;
		lderr(oc->cct) << " Failed to create sub process: " << cmd
				       << " err msg: " << cpp_strerror(r) << dendl;
		return -r;
	}
	ldout(oc->cct, 11) << " Succeed to create sub process: " << cmd << dendl;

	/* iscsiadm -m node -T {target-iqn} -p {target-ip} -l
	Success output: first login  
	 	Logging in to [iface: iface0, target: iqn.2016-12.com.ceph03:disk1, portal: 10.10.5.23,3260] (multiple)
	 	Login to [iface: iface0, target: iqn.2016-12.com.ceph03:disk1, portal: 10.10.5.23,3260] successful.

	 	NOTE: there is no output(blank) for later success login
	*/
	while (fgets(output, sizeof(output), pf) != NULL) {
		lderr(oc->cct) << " output: " << output << dendl;
		if (strstr(output, iqn) == NULL) {
			pclose(pf);
			lderr(oc->cct) << " Failed to login!!!" << dendl;
			return -1;
		}
	}

	pclose(pf);
	ldout(oc->cct, 11) << " Succeed to login" << dendl; 
	return 0;
}

int cetcd_client::cetcd_get_device(const char *iqn, char *devname, int size)
{
	/*
	1. get session id 
	
	iscsiadm -m session 
	tcp: [2] 10.130.120.15:3260,1 iqn.2017-11.com.compute02:disk1 (non-flash)

    2. get iscsi device
    
	iscsiadm -m session -P 3 -n {iqn}

	************************
	Attached SCSI devices:
	************************
	Host Number: 44 State: running
	scsi44 Channel 00 Id 0 Lun: 0
		Attached scsi disk sdo		State: running
	*/
	if (NULL == iqn ||
		NULL == devname || size <= 0) {
		return -EINVAL;
	}
	
	FILE *pf = NULL;
	int sid = -1;
	char cmd[256] = {0};
	char output[1024] = {0};

	snprintf(cmd, sizeof(cmd), "iscsiadm -m session | grep %s"
		" | awk -F'[' '{print $2}' | awk -F']' '{print $1}'", iqn);
	pf = popen(cmd, "r");
	if (NULL == pf) {
		int r = errno;
		lderr(oc->cct) << " Failed to create sub process: " << cmd
					   << " err msg: " << cpp_strerror(r) << dendl;
		return -r;
	}
	ldout(oc->cct, 11) << " Succeed to create sub process: " << cmd << dendl;

	if (fgets(output, sizeof(output), pf) != NULL) {
		sid = atoi((char*)output);
		ldout(oc->cct, 11) << " iscsi session id " << sid << dendl;
	}	

	if (sid == -1) {
		int r = ferror(pf);
		lderr(oc->cct) << " Failed to get iscsi session id" 
					   << " err msg: " << cpp_strerror(r) << dendl;
		pclose(pf);
		return -r;
	}
	pclose(pf);

	cmd[256] = {0};
	snprintf(cmd, sizeof(cmd), "iscsiadm -m session -r %d -P 3" 
		" | grep -e \"Attached scsi disk\"|cut -d ' ' -f 4", sid);
	pf = popen(cmd, "r");
	if (NULL == pf) {
		int r = errno;
		lderr(oc->cct) << " Failed to create sub-process: " << cmd
					   << " err msg: " << cpp_strerror(r) << dendl;
		return -r;
	}
	ldout(oc->cct, 11) << " Succeed to create sub process: " << cmd << dendl;

	output[1024] = {0};
	if (fgets(output, sizeof(output), pf) != NULL) {
		//sdo\t\tState: running\n
		char *p = output;

		while (*p >= 'a' && *p <= 'z') ++p;
		*p = '\0';
		snprintf(devname, size, "/dev/%s", output);
		
		pclose(pf);
		ldout(oc->cct, 11) << " Succeed to get device Name: " << devname << dendl;
		return 0;
	}

	int r = ferror(pf);
	lderr(oc->cct) << " Failed to get device" 
				   << " err msg: " << cpp_strerror(r) << dendl;
	pclose(pf);
	return -r;
}

int cetcd_client::cetcd_convert_device(const char *uuid, char *devname, int size)
{
	//lsblk --fs -l|grep {uuid}
	if (NULL == uuid ||
		NULL == devname || size <= 0)
		return -EINVAL;
	
	FILE *pf = NULL;
	char cmd[256] = {0};
	snprintf(cmd, sizeof(cmd), "/usr/bin/lsblk --fs -l|grep %s", uuid);

	pf = popen(cmd, "r");
	if (NULL == pf) {
		int r = errno;
		lderr(oc->cct) << " Failed to create sub-process: " << cmd 
			<< ", err msg: " << cpp_strerror(r) << dendl;
		return -r;
	}
			
	char output[1024] = {0};
	if (fgets(output, sizeof(output), pf) != NULL) {
		ldout(oc->cct, 7) << " output: " << output << dendl;

		char *ptr = strtok(output, " ");
		assert(ptr);
		snprintf(devname, size, "/dev/%s", ptr);

		pclose(pf);
		ldout(oc->cct, 11) << " Succeed to convert uuid: " << uuid 
			<< " to device name: " << devname << dendl;
		return 0;
	} 

	lderr(oc->cct) << " Failed to convert uuid to device name"
		<< ", err msg: " << cpp_strerror(ENXIO)<< dendl;
	pclose(pf);
	return -ENXIO;	
}

int cetcd_client::cetcd_curl_setopt(CURL *curl, cetcd_watcher *watcher) 
{ 
    cetcd_string url;

    url = cetcd_watcher_build_url(watcher);
    curl_easy_setopt(curl,CURLOPT_URL, url);
    sdsfree(url);

    /* See above about CURLOPT_NOSIGNAL
     * */
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, watcher->cli->settings.connect_timeout);
#if LIBCURL_VERSION_NUM >= 0x071900
    curl_easy_setopt(watcher->curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(watcher->curl, CURLOPT_TCP_KEEPINTVL, 1L); /*the same as go-etcd*/
#endif
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "cetcd");
    curl_easy_setopt(curl, CURLOPT_POSTREDIR, 3L);     /*post after redirecting*/
    curl_easy_setopt(curl, CURLOPT_VERBOSE, watcher->cli->settings.verbose);

    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, cetcd_parse_response);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, watcher->parser);
    curl_easy_setopt(curl, CURLOPT_HEADER, 1L);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    curl_easy_setopt(curl, CURLOPT_PRIVATE, watcher);
    watcher->curl = curl;

    return 1;
}

int cetcd_client::cetcd_add_watcher(cetcd_array *watchers, cetcd_watcher *watcher) 
{
    cetcd_watcher *w;

    cetcd_curl_setopt(watcher->curl, watcher);

    watcher->attempts = cetcd_array_size(watcher->cli->addresses);
    /* We use an array to store watchers. It will cause holes when remove some watchers.
     * watcher->array_index is used to reset to the original hole if the watcher was deleted before.
     * */
    if (watcher->array_index == -1) {
        cetcd_array_append(watchers, watcher);
        watcher->array_index = cetcd_array_size(watchers) - 1;
    } else {
        w = (cetcd_watcher*)cetcd_array_get(watchers, watcher->array_index);
        if (w) {
            cetcd_watcher_release(w);
        }
        cetcd_array_set(watchers, watcher->array_index, watcher);
    }
    return 1;
}

int cetcd_client::cetcd_del_watcher(cetcd_array *watchers, cetcd_watcher *watcher) 
{
    int index;
    index = watcher->array_index;
    if (watcher && index >= 0) {
        cetcd_array_set(watchers, index, NULL);
        cetcd_watcher_release(watcher);
    }
    return 1;
}

int cetcd_client::cetcd_stop_watcher(cetcd_watcher *watcher) 
{
    /* Clear the callback function pointer to ensure to stop notify the user
     * Set once to 1 indicates that the watcher would stop after next trigger.
     *
     * The watcher object would be freed by cetcd_reap_watchers
     * Watchers may hang forever if it would be never triggered after set once to 1
     * FIXME: Cancel the blocking watcher
     * */
    watcher->callback = NULL;
    watcher->once = 1;
    return 1;
}

int cetcd_client::cetcd_reap_watchers(CURLM *mcurl) 
{
    uint64_t index;
    int     added, ignore;
    CURLMsg *msg;
    CURL    *curl;
    cetcd_string url;
    cetcd_watcher *watcher;
    cetcd_response *resp;
    added = 0;
    index = 0;
    while ((msg = curl_multi_info_read(mcurl, &ignore)) != NULL) {
        if (msg->msg == CURLMSG_DONE) {
            curl = msg->easy_handle;
            curl_easy_getinfo(curl, CURLINFO_PRIVATE, &watcher);

            resp = (cetcd_response*)watcher->parser->resp;
            index = watcher->index;
            if (msg->data.result != CURLE_OK) {
                /*try next in round-robin ways*/
                /*FIXME There is a race condition if multiple watchers failed*/
                if (watcher->attempts) {
                    picked = (picked+1)%(cetcd_array_size(addresses));
                    url = cetcd_watcher_build_url(watcher);
                    curl_easy_setopt(watcher->curl, CURLOPT_URL, url);
                    sdsfree(url);
                    curl_multi_remove_handle(mcurl, curl);
                    watcher->parser->st = 0;
                    curl_easy_reset(curl);
                    cetcd_curl_setopt(curl, watcher);
                    curl_multi_add_handle(mcurl, curl);
                    /*++added;
                     *watcher->attempts --;
                     */
                    continue;
                } else {
                    resp->err = (cetcd_error*)calloc(1, sizeof(cetcd_error));
                    resp->err->ecode = error_cluster_failed;
                    resp->err->message = sdsnew("cetcd_reap_watchers: all cluster servers failed.");
                }
            }
            if (watcher->callback) {
                watcher->callback(watcher->userdata, resp);
                if (resp->err && resp->err->ecode != 401/* not outdated*/) {
                    curl_multi_remove_handle(mcurl, curl);
                    cetcd_watcher_release(watcher);
                    break;
                }
                if (resp->node) {
                    index = resp->node->modified_index;
                } else {
                    ++ index;
                }
                cetcd_response_release(resp);
                watcher->parser->resp = NULL; /*surpress it be freed again by cetcd_watcher_release*/
            }
            if (!watcher->once) {
                curl_multi_remove_handle(mcurl, curl);
                cetcd_watcher_reset(watcher);

                if (watcher->index) {
                    watcher->index = index + 1;
                    url = cetcd_watcher_build_url(watcher);
                    curl_easy_setopt(watcher->curl, CURLOPT_URL, url);
                    sdsfree(url);
                }
                curl_multi_add_handle(mcurl, watcher->curl);
                ++added;
                continue;
            }
            curl_multi_remove_handle(mcurl, curl);
            cetcd_watcher_release(watcher);
        }
    }
    return added;
}

int cetcd_client::cetcd_multi_watch(cetcd_array *watchers) 
{
    int           i, count;
    int           maxfd, left, added;
    long          timeout;
    long          backoff, backoff_max;
    fd_set        r, w, e;
    cetcd_watcher *watcher;
    CURLM         *mcurl;

    struct timeval tv;

    mcurl = curl_multi_init();
    count = cetcd_array_size(watchers);
    for (i = 0; i < count; ++i) {
        watcher = (cetcd_watcher*)cetcd_array_get(watchers, i);
        curl_easy_setopt(watcher->curl, CURLOPT_PRIVATE, watcher);
        curl_multi_add_handle(mcurl, watcher->curl);
    }
    backoff = 100; /*100ms*/
    backoff_max = 1000; /*1 sec*/
    for(;;) {
        curl_multi_perform(mcurl, &left);
        if (left) {
            FD_ZERO(&r);
            FD_ZERO(&w);
            FD_ZERO(&e);

            curl_multi_timeout(mcurl, &timeout);
            if (timeout == -1) {
                timeout = 100; /*wait for 0.1 seconds*/
            }
            tv.tv_sec = timeout/1000;
            tv.tv_usec = (timeout%1000)*1000;

            curl_multi_fdset(mcurl, &r, &w, &e, &maxfd);

            /*TODO handle errors*/
            select(maxfd+1, &r, &w, &e, &tv);

            curl_multi_perform(mcurl, &left);
        }
        added = cetcd_reap_watchers(mcurl);
        if (added == 0 && left == 0) {
        /* It will call curl_multi_perform immediately if:
         * 1. left is 0
         * 2. a new attempt should be issued
         * It is expected to sleep a mount time between attempts.
         * So we fix this by increasing added counter only
         * when a new request should be issued.
         * When added is 0, maybe there are retring requests or nothing.
         * Either situations should wait before issuing the request.
         * */
            if (backoff < backoff_max) {
                backoff = 2 * backoff;
            } else {
                backoff = backoff_max;
            }
            tv.tv_sec = backoff/1000;
            tv.tv_usec = (backoff%1000) * 1000;
            select(1, 0, 0, 0, &tv);
        }
    }
    curl_multi_cleanup(mcurl);
    return count;
}

static void *cetcd_multi_watch_wrapper(void *args[]) 
{
    cetcd_client *cli;
    cetcd_array  *watchers;
    cli = (cetcd_client*)args[0];
    watchers = (cetcd_array*)args[1];
    free(args);
    cli->cetcd_multi_watch(watchers);
    return 0;
}

cetcd_watch_id cetcd_client::cetcd_multi_watch_async(cetcd_array *watchers) 
{
    pthread_t thread;
    void **args;
    args = (void **)calloc(2, sizeof(void *));
    args[0] = this;
    args[1] = watchers;
    pthread_create(&thread, NULL, (void *(*)(void *))cetcd_multi_watch_wrapper, args);
    return thread;
}

int cetcd_client::cetcd_multi_watch_async_stop(cetcd_watch_id wid) 
{
    /* Cancel causes the thread exit immediately, so the resouce has been
     * allocted won't be freed. The memory leak is OK because the process
     * is going to exit.
     * TODO fix the memory leaks
     * */
    pthread_cancel(wid);
    pthread_join(wid, 0);
    return 0;
}

cetcd_response *cetcd_client::cetcd_get(const char *key) 
{
    return cetcd_lsdir(key, 0, 0);
}

cetcd_response *cetcd_client::cetcd_lsdir(const char *key, int sort, int recursive) 
{
    cetcd_request req;
    cetcd_response *resp;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_GET;
    req.uri = sdscatprintf(sdsempty(), "%s%s", keys_space, key);
    if (sort) {
        req.uri = sdscatprintf(req.uri, "?sorted=true");
    }
    if (recursive){
        req.uri = sdscatprintf(req.uri, "%crecursive=true", sort?'&':'?');
    }
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    return resp;
}

cetcd_response *cetcd_client::cetcd_set(const char *key, const char *value, uint64_t ttl) 
{
    cetcd_request req;
    cetcd_response *resp;
    cetcd_string params;
    char *value_escaped;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_PUT;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s", keys_space, key);
    value_escaped = curl_easy_escape(curl, value, strlen(value));
    params = sdscatprintf(sdsempty(), "value=%s", value_escaped);
    curl_free(value_escaped);
    if (ttl) {
        params = sdscatprintf(params, "&ttl=%lu", ttl);
    }
    req.data = params;
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    sdsfree(params);
    return resp;
}

cetcd_response *cetcd_client::cetcd_mkdir(const char *key, uint64_t ttl)
{
    cetcd_request req;
    cetcd_response *resp;
    cetcd_string params;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_PUT;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s", keys_space, key);
    params = sdscatprintf(sdsempty(), "dir=true&prevExist=false");
    if (ttl) {
        params = sdscatprintf(params, "&ttl=%lu", ttl);
    }
    req.data = params;
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    sdsfree(params);
    return resp;
}

cetcd_response *cetcd_client::cetcd_setdir(const char *key, uint64_t ttl)
{
    cetcd_request req;
    cetcd_response *resp;
    cetcd_string params;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_PUT;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s", keys_space, key);
    params = sdscatprintf(sdsempty(), "dir=true");
    if (ttl) {
        params = sdscatprintf(params, "&ttl=%lu", ttl);
    }
    req.data = params;
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    sdsfree(params);
    return resp;
}

cetcd_response *cetcd_client::cetcd_updatedir(const char *key, uint64_t ttl)
{
    cetcd_request req;
    cetcd_response *resp;
    cetcd_string params;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_PUT;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s", keys_space, key);
    params = sdscatprintf(sdsempty(), "dir=true&prevExist=true");
    if (ttl){
        params = sdscatprintf(params, "&ttl=%lu", ttl);
    }
    req.data = params;
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    sdsfree(params);
    return resp;
}

cetcd_response *cetcd_client::cetcd_update(const char *key,
                             const char *value, uint64_t ttl, int refresh) 
{
    cetcd_request req;
    cetcd_response *resp;
    cetcd_string params;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_PUT;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s", keys_space, key);
    params = sdscatprintf(sdsempty(), "prevExist=true");
    if (value) {
        char *value_escaped;
        value_escaped = curl_easy_escape(curl, value, strlen(value));
        params = sdscatprintf(params, "&value=%s", value_escaped);
        curl_free(value_escaped);
    }
    if (ttl) {
        params = sdscatprintf(params, "&ttl=%lu", ttl);
    }
    if (refresh) {
        params = sdscatprintf(params, "&refresh=true");
    }
    req.data = params;
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    sdsfree(params);
    return resp;
}

cetcd_response *cetcd_client::cetcd_create(const char *key,
        const char *value, uint64_t ttl)
{
    cetcd_request req;
    cetcd_response *resp;
    cetcd_string params;
    char *value_escaped;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_PUT;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s", keys_space, key);
    value_escaped = curl_easy_escape(curl, value, strlen(value));
    params = sdscatprintf(sdsempty(), "prevExist=false&value=%s", value_escaped);
    curl_free(value_escaped);
    if (ttl) {
        params = sdscatprintf(params, "&ttl=%lu", ttl);
    }
    req.data = params;
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    sdsfree(params);
    return resp;
}

cetcd_response *cetcd_client::cetcd_create_in_order(const char *key,
        const char *value, uint64_t ttl)
{
    cetcd_request req;
    cetcd_response *resp;
    cetcd_string params;
    char *value_escaped;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_POST;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(),"%s%s", keys_space, key);
    value_escaped = curl_easy_escape(curl, value, strlen(value));
    params = sdscatprintf(sdsempty(), "value=%s", value_escaped);
    curl_free(value_escaped);
    if (ttl){
        params = sdscatprintf(params ,"&ttl=%lu", ttl);
    }
    req.data = params;
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    sdsfree(params);
    return resp;
}

cetcd_response *cetcd_client::cetcd_delete(const char *key) 
{
    cetcd_request req;
    cetcd_response *resp;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_DELETE;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s", keys_space, key);
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    return resp;
}

cetcd_response *cetcd_client::cetcd_rmdir(const char *key, int recursive)
{
    cetcd_request req;
    cetcd_response *resp;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_DELETE;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s?dir=true", keys_space, key);
    if (recursive){
        req.uri = sdscatprintf(req.uri, "&recursive=true");
    }
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    return resp;
}

cetcd_response *cetcd_client::cetcd_watch(const char *key, uint64_t index) 
{
    cetcd_request req;
    cetcd_response *resp;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_GET;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s?wait=true&waitIndex=%lu", keys_space, key, index);
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    return resp;
}

cetcd_response *cetcd_client::cetcd_watch_recursive(const char *key, uint64_t index) 
{
    cetcd_request req;
    cetcd_response *resp;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_GET;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s?wait=true&recursive=true&waitIndex=%lu", keys_space, key, index);
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    return resp;
}

cetcd_response *cetcd_client::cetcd_cmp_and_swap(const char *key, const char *value, const char *prev, uint64_t ttl) 
{
    cetcd_request req;
    cetcd_response *resp;
    cetcd_string params;
    char *value_escaped;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_PUT;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s", keys_space, key);
    value_escaped = curl_easy_escape(curl, value, strlen(value));
    params = sdscatprintf(sdsempty(), "value=%s&prevValue=%s", value_escaped, prev);
    curl_free(value_escaped);
    if (ttl) {
        params = sdscatprintf(params, "&ttl=%lu", ttl);
    }
    req.data = params;
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    sdsfree(params);
    return resp;
}

cetcd_response *cetcd_client::cetcd_cmp_and_swap_by_index(const char *key, const char *value, uint64_t prev, uint64_t ttl) 
{
    cetcd_request req;
    cetcd_response *resp;
    cetcd_string params;
    char *value_escaped;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_PUT;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s", keys_space, key);
    value_escaped = curl_easy_escape(curl, value, strlen(value));
    params = sdscatprintf(sdsempty(), "value=%s&prevIndex=%lu", value_escaped, prev);
    curl_free(value_escaped);
    if (ttl) {
        params = sdscatprintf(params, "&ttl=%lu", ttl);
    }
    req.data = params;
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    sdsfree(params);
    return resp;
}

cetcd_response *cetcd_client::cetcd_cmp_and_delete(const char *key, const char *prev) 
{
    cetcd_request req;
    cetcd_response *resp;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_DELETE;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s?prevValue=%s", keys_space, key, prev);
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    return resp;
}

cetcd_response *cetcd_client::cetcd_cmp_and_delete_by_index(const char *key, uint64_t prev) 
{
    cetcd_request req;
    cetcd_response *resp;

    memset(&req, 0, sizeof(cetcd_request));
    req.method = ETCD_HTTP_DELETE;
    req.api_type = ETCD_KEYS;
    req.uri = sdscatprintf(sdsempty(), "%s%s?prevIndex=%lu", keys_space, key, prev);
    resp = (cetcd_response*)cetcd_cluster_request(&req);
    sdsfree(req.uri);
    return resp;
}

void cetcd_client::cetcd_node_release(cetcd_response_node *node) 
{
    int i, count;
    cetcd_response_node *n;
    if (node->nodes) {
        count = cetcd_array_size(node->nodes);
        for (i = 0; i < count; ++i) {
            n = (cetcd_response_node*)cetcd_array_get(node->nodes, i);
            cetcd_node_release(n);
        }
        cetcd_array_release(node->nodes);
    }
    if (node->key) {
        sdsfree(node->key);
    }
    if (node->value) {
        sdsfree(node->value);
    }
    free(node);
}

void cetcd_client::cetcd_response_release(cetcd_response *resp) 
{
    if(resp) {
        if (resp->err) {
            cetcd_error_release(resp->err);
            resp->err = NULL;
        }
        if (resp->node) {
            cetcd_node_release(resp->node);
        }
        if (resp->prev_node) {
            cetcd_node_release(resp->prev_node);
        }
        free(resp);
    }
}

void cetcd_client::cetcd_error_release(cetcd_error *err) 
{
    if (err) {
        if (err->message) {
            sdsfree(err->message);
        }
        if (err->cause) {
            sdsfree(err->cause);
        }
        free(err);
    }
}

void cetcd_client::cetcd_node_print(cetcd_response_node *node) 
{
    int i, count;
    cetcd_response_node *n;
    if (node) {
        ldout(oc->cct, 11)<< "Node TTL: " << node->ttl << dendl;
        ldout(oc->cct, 11)<< "Node ModifiedIndex: " << node->modified_index << dendl;
        ldout(oc->cct, 11)<< "Node CreatedIndex: " << node->created_index << dendl;
        ldout(oc->cct, 11)<< "Node Key: " << node->key << dendl;
        ldout(oc->cct, 11)<< "Node Value: " << node->value << dendl;
        ldout(oc->cct, 11)<< "Node Dir: " << node->dir << dendl;
		//node->key format: /{prefix}:{host}/ip:{priority} {priority} = 0-slave, 1-master
		if (strstr(node->key, cache_path.c_str()) && !node->dir) {
			char ip[64] = {0};
			int start = strlen(cache_path.c_str()) + 1;
			int last = strlen(node->key) - 1 - 1;
			
			strncpy(ip, node->key+start, last-start);
			server_dev_map[ip] = node->value;
			server_role_map[ip] = node->key[strlen(node->key)-1] == '1'? 1 : 0; 
		}
		
        if (node->nodes) {
            count = cetcd_array_size(node->nodes);
            for (i = 0; i < count; ++i) {
                n = (cetcd_response_node*)cetcd_array_get(node->nodes, i);
                cetcd_node_print(n);
            }
        }
    }
}

void cetcd_client::cetcd_response_print(cetcd_response *resp) 
{
    if (resp->err) {
        lderr(oc->cct)<< "Error Code: " << resp->err->ecode << dendl;
        lderr(oc->cct)<< "Error Message: " << resp->err->message << dendl;
        lderr(oc->cct)<< "Error Cause: " << resp->err->cause << dendl;
        return;
    }
    ldout(oc->cct, 11)<< "Etcd Action: " << cetcd_event_action[resp->action] << dendl;
    ldout(oc->cct, 11)<< "Etcd Index: " << resp->etcd_index << dendl;
    ldout(oc->cct, 11)<< "Raft Index: " << resp->raft_index << dendl;
    ldout(oc->cct, 11)<< "Raft Term: " << resp->raft_term << dendl;
    if (resp->node) {
        ldout(oc->cct, 11)<< "-------------Node------------" << dendl;
        cetcd_node_print(resp->node);
    }
    if (resp->prev_node) {
        ldout(oc->cct, 11)<< "-------------PreNode------------" << dendl;
        cetcd_node_print(resp->prev_node);
    }
}

size_t cetcd_parse_response(char *ptr, size_t size, size_t nmemb, void *userdata) 
{
    int len, i;
    char *key, *val;
    cetcd_response_parser *parser;
    yajl_status status;
    cetcd_response *resp = NULL;
    cetcd_array *addrs = NULL;

    enum resp_parser_st {
        request_line_start_st,
        request_line_end_st,
        request_line_http_status_start_st,
        request_line_http_status_st,
        request_line_http_status_end_st,
        header_key_start_st,
        header_key_st,
        header_key_end_st,
        header_val_start_st,
        header_val_st,
        header_val_end_st,
        blank_line_st,
        json_start_st,
        json_end_st,
        response_discard_st
    };
    /* Headers we are interested in:
     * X-Etcd-Index: 14695
     * X-Raft-Index: 672930
     * X-Raft-Term: 12
     * */
    parser = (cetcd_response_parser*)userdata;
    if (parser->api_type == ETCD_MEMBERS) {
        addrs = (cetcd_array*)parser->resp;
    } else {
        resp = (cetcd_response*)parser->resp;
    }
    len = size * nmemb;
    for (i = 0; i < len; ++i) {
        if (parser->st == request_line_start_st) {
            if (ptr[i] == ' ') {
                parser->st = request_line_http_status_start_st;
            }
            continue;
        }
        if (parser->st == request_line_end_st) {
            if (ptr[i] == '\n') {
                parser->st = header_key_start_st;
            }
            continue;
        }
        if (parser->st == request_line_http_status_start_st) {
            parser->buf = sdscatlen(parser->buf, ptr+i, 1);
            parser->st = request_line_http_status_st;
            continue;
        }
        if (parser->st == request_line_http_status_st) {
            if (ptr[i] == ' ') {
                parser->st = request_line_http_status_end_st;
            } else {
                parser->buf = sdscatlen(parser->buf, ptr+i, 1);
                continue;
            }
        }
        if (parser->st == request_line_http_status_end_st) {
            val = parser->buf;
            parser->http_status = atoi(val);
            sdsclear(parser->buf);
            parser->st = request_line_end_st;
            if (parser->api_type == ETCD_MEMBERS && parser->http_status != 200) {
                parser->st = response_discard_st;
            }
            continue;
        }
        if (parser->st == header_key_start_st) {
            if (ptr[i] == '\r') {
                ++i;
            }
            if (ptr[i] == '\n') {
                parser->st = blank_line_st;
                if (parser->http_status >= 300 && parser->http_status < 400) {
                    /*this is a redirection, restart the state machine*/
                    parser->st = request_line_start_st;
                    break;
                }
                continue;
            }
            parser->st = header_key_st;
        }
        if (parser->st == header_key_st) {
            parser->buf = sdscatlen(parser->buf, ptr+i, 1);
            if (ptr[i] == ':') {
                parser->st = header_key_end_st;
            } else {
                continue;
            }
        }
        if (parser->st == header_key_end_st) {
            parser->st = header_val_start_st;
            continue;
        }
        if (parser->st == header_val_start_st) {
            if (ptr[i] == ' ') {
                continue;
            }
            parser->st = header_val_st;
        }
        if (parser->st == header_val_st) {
            if (ptr[i] == '\r') {
                ++i;
            }
            if (ptr[i] == '\n') {
                parser->st = header_val_end_st;
            } else {
                parser->buf = sdscatlen(parser->buf, ptr+i, 1);
                continue;
            }
        }
        if (parser->st == header_val_end_st) {
            parser->st = header_key_start_st;
            if (parser->api_type == ETCD_MEMBERS) {
                sdsclear(parser->buf);
                continue;
            }
            int count = 0;
            sds *kvs = sdssplitlen(parser->buf, sdslen(parser->buf), ":", 1, &count);
            sdsclear(parser->buf);
            if (count < 2) {
                sdsfreesplitres(kvs, count);
                continue;
            }
            key = kvs[0];
            val = kvs[1];
            if (strncmp(key, "X-Etcd-Index", sizeof("X-Etcd-Index")-1) == 0) {
                resp->etcd_index = atoi(val);
            } else if (strncmp(key, "X-Raft-Index", sizeof("X-Raft-Index")-1) == 0) {
                resp->raft_index = atoi(val);
            } else if (strncmp(key, "X-Raft-Term", sizeof("X-Raft-Term")-1) == 0) {
                resp->raft_term = atoi(val);
            }
            sdsfreesplitres(kvs, count);
            continue;
        }
        if (parser->st == blank_line_st) {
            if (ptr[i] != '{') {
                /*not a json response, discard*/
                parser->st = response_discard_st;
                if (resp->err == NULL && parser->api_type == ETCD_KEYS) {
                    resp->err = (cetcd_error*)calloc(1, sizeof(cetcd_error));
                    resp->err->ecode = error_response_parsed_failed;
                    resp->err->message = sdsnew("not a json response");
                    resp->err->cause = sdsnewlen(ptr, len);
                }
                continue;
            }
            parser->st = json_start_st;
            cetcd_array_init(&parser->ctx.keystack, 10);
            cetcd_array_init(&parser->ctx.nodestack, 10);
            if (parser->api_type == ETCD_MEMBERS) {
                parser->ctx.userdata = addrs;
                parser->json = yajl_alloc(&sync_callbacks, 0, &parser->ctx);
            }
            else {
                if (parser->http_status != 200 && parser->http_status != 201) {
                    resp->err = (cetcd_error*)calloc(1, sizeof(cetcd_error));
                    parser->ctx.userdata = resp->err;
                    parser->json = yajl_alloc(&error_callbacks, 0, &parser->ctx);
                } else {
                    parser->ctx.userdata = resp;
                    parser->json = yajl_alloc(&callbacks, 0, &parser->ctx);
                }
            }
        }
        if (parser->st == json_start_st) {
            if (yajl_status_ok == yajl_parse(parser->json, (const unsigned char *)ptr + i, len - i)) {
                //all text left has been parsed, break the for loop
                break;
            } else {
                parser->st = json_end_st;
            }
        }
        if (parser->st == json_end_st) {
            status = yajl_complete_parse(parser->json);
            /*parse failed, TODO set error message*/
            if (status != yajl_status_ok) {
                if ( parser->api_type == ETCD_KEYS && resp->err == NULL) {
                    resp->err = (cetcd_error*)calloc(1, sizeof(cetcd_error));
                    resp->err->ecode = error_response_parsed_failed;
                    resp->err->message = sdsnew("http response is invalid json format");
                    resp->err->cause = sdsnewlen(ptr, len);
                }
                return 0;
            }
            break;
        }
        if (parser->st == response_discard_st) {
            return len;
        }
    }
    return len;
}

void *cetcd_client::cetcd_send_request(CURL *curl, cetcd_request *req) 
{
    CURLcode res;
    cetcd_response_parser parser;
    cetcd_response *resp = NULL ;
    cetcd_array *addrs = NULL;

    if (req->api_type == ETCD_MEMBERS) {
        addrs = cetcd_array_create(10);
        parser.resp = addrs;
    } else {
        resp = (cetcd_response*)calloc(1, sizeof(cetcd_response));
        parser.resp = resp;
    }

    parser.api_type = req->api_type;
    parser.st = 0; /*0 should be the start state of the state machine*/
    parser.buf = sdsempty();
    parser.json = NULL;

    curl_easy_setopt(curl, CURLOPT_URL, req->url);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, http_method[req->method]);
    if (req->method == ETCD_HTTP_PUT || req->method == ETCD_HTTP_POST) {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, req->data);
    } else {
        /* We must clear post fields here:
         * We reuse the curl handle for all HTTP methods.
         * CURLOPT_POSTFIELDS would be set when issue a PUT request.
         * The field  pointed to the freed req->data. It would be
         * reused by next request.
         * */
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "");
    }
    if (req->cli->settings.user) {
        curl_easy_setopt(curl, CURLOPT_USERNAME, req->cli->settings.user);
    }
    if (req->cli->settings.password) {
        curl_easy_setopt(curl, CURLOPT_PASSWORD, req->cli->settings.password);
    }
    curl_easy_setopt(curl, CURLOPT_HEADER, 1L);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &parser);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, cetcd_parse_response);
    curl_easy_setopt(curl, CURLOPT_VERBOSE, req->cli->settings.verbose);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, req->cli->settings.connect_timeout);
    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, "Expect:");
    res = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

    res = curl_easy_perform(curl);

    curl_slist_free_all(chunk);
    //release the parser resource
    sdsfree(parser.buf);
    if (parser.json) {
        yajl_free(parser.json);
        cetcd_array_destroy(&parser.ctx.keystack);
        cetcd_array_destroy(&parser.ctx.nodestack);
    }

    if (res != CURLE_OK) {
        if (req->api_type == ETCD_MEMBERS) {
            return addrs;
        }
        if (resp->err == NULL) {
            resp->err = (cetcd_error*)calloc(1, sizeof(cetcd_error));
            resp->err->ecode = error_send_request_failed;
            resp->err->message = sdsnew(curl_easy_strerror(res));
            resp->err->cause = sdsdup(req->url);
        }
        return resp;
    }
    return parser.resp;
}

/*
 * cetcd_cluster_request tries to request the whole cluster. It round-robin to next server if the request failed
 * */
void *cetcd_client::cetcd_cluster_request(cetcd_request *req) 
{
    size_t i, count;
    cetcd_string url;
    cetcd_error *err = NULL;
    cetcd_response *resp = NULL;
    cetcd_array *addrs = NULL;
    void *res = NULL;

    count = cetcd_array_size(addresses);

    for(i = 0; i < count; ++i) {
        url = sdscatprintf(sdsempty(), "%s/%s", (cetcd_string)cetcd_array_get(addresses, picked), req->uri);
        req->url = url;
        req->cli = this;
        res = cetcd_send_request(curl, req);
        sdsfree(url);

        if (req->api_type == ETCD_MEMBERS) {
            addrs = (cetcd_array*)res;
            /* Got the result addresses, return*/
            if (addrs && cetcd_array_size(addrs)) {
                return addrs;
            }
            /* Empty or error ? retry */
            if (addrs) {
                cetcd_array_release(addrs);
                addrs = NULL;
            }
            if (i == count - 1) {
                break;
            }
        } else if (req->api_type == ETCD_KEYS) {
            resp = (cetcd_response*)res;
            if(resp && resp->err && resp->err->ecode == error_send_request_failed) {
                if (i == count - 1) {
                    break;
                }
            cetcd_response_release(resp);
            } else {
                /*got response, return*/
                return resp;
            }

        }
        /*try next*/
        picked = (picked + 1) % count;
    }
    /*the whole cluster failed*/
    if (req->api_type == ETCD_MEMBERS) return NULL;
    if (resp) {
        if(resp->err) {
            err = resp->err; /*remember last error*/
        }
        resp->err = (cetcd_error*)calloc(1, sizeof(cetcd_error));
        resp->err->ecode = error_cluster_failed;
        resp->err->message = sdsnew("etcd_cluster_request: all cluster servers failed.");
        if (err) {
            resp->err->message = sdscatprintf(resp->err->message, " last error: %s", err->message);
            cetcd_error_release(err);
        }
        resp->err->cause = sdsdup(req->uri);
    }
    return resp;
}
