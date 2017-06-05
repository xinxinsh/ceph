#define YAJL_MAJOR 2
#define YAJL_MINOR 1
#define YAJL_MICRO 0

#define YAJL_VERSION ((YAJL_MAJOR * 10000) + (YAJL_MINOR * 100) + YAJL_MICRO)

int yajl_version(void)
{
	
	return YAJL_VERSION;
}

