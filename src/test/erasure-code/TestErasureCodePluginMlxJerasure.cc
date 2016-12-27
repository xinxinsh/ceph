#include <errno.h>
#include <stdlib.h>
#include "global/global_init.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "common/ceph_argparse.h"
#include "log/Log.h"
#include "global/global_context.h"
#include "common/config.h"
#include "gtest/gtest.h"

#define DEFAULT_MLX_NIC "mlx5_0"

TEST(ErasureCodePlugin, factory)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodeProfile profile;
  {
    ErasureCodeInterfaceRef erasure_code;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(-ENOENT, instance.factory("mlxjerasure",
					g_conf->erasure_code_dir,
					profile,
                                        &erasure_code, &cerr));
    EXPECT_FALSE(erasure_code);
  }
  {
    ErasureCodeInterfaceRef erasure_code;
    profile["nic"] = DEFAULT_MLX_NIC;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(0, instance.factory("mlxjerasure",
                                  g_conf->erasure_code_dir,
                                  profile,
                                  &erasure_code, &cerr));
    EXPECT_TRUE(erasure_code.get());
  }
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const char* env = getenv("CEPH_LIB");
  string directory(env ? env : ".libs");
  g_conf->set_val("erasure_code_dir", directory, false, false);

  ::testing::InitGoogleTest(&argc, argv);
  int status = RUN_ALL_TESTS();
  g_ceph_context->_log->stop();
  return status;
}
