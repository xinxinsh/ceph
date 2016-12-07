set(mlxjerasure_obj_srcs
  jerasure/src/galois.c
  jerasure/src/jerasure.c
  jerasure/src/reed_sol.c
  gf-complete/src/gf_wgen.c
  gf-complete/src/gf_method.c
  gf-complete/src/gf_w16.c
  gf-complete/src/gf.c
  gf-complete/src/gf_w32.c
  gf-complete/src/gf_w64.c
  gf-complete/src/gf_w128.c
  gf-complete/src/gf_general.c
  gf-complete/src/gf_w4.c
  gf-complete/src/gf_rand.c
  gf-complete/src/gf_w8.c
  )
add_library(mlxjerasure_objs OBJECT ${jerasure_obj_srcs})

add_library(ec_mlxjerasure_objs OBJECT
  ErasureCodePluginMlxJerasure.cc
  ErasureCodeMlxJerasure.cc
  ErasureCode_MlxJerasure_common.cc
  )

add_library(ec_mlxjerasure SHARED
  $<TARGET_OBJECTS:ec_mlxjerasure_objs>
  $<TARGET_OBJECTS:mlxjerasure_objs>
  $<TARGET_OBJECTS:erasure_code_objs>
  )
add_dependencies(ec_mlxjerasure ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_mlxjerasure crush ${EXTRALIBS})
set_target_properties(ec_mlxjerasure PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ec_mlxjerasure DESTINATION lib/erasure-code)

