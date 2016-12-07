noinst_HEADERS += \
  erasure-code/jerasure/gf-complete/include/gf_complete.h \
  erasure-code/jerasure/gf-complete/include/gf_general.h \
  erasure-code/jerasure/gf-complete/include/gf_int.h \
  erasure-code/jerasure/gf-complete/include/gf_method.h \
  erasure-code/jerasure/gf-complete/include/gf_rand.h \
  erasure-code/jerasure/gf-complete/include/gf_w16.h \
  erasure-code/jerasure/gf-complete/include/gf_w32.h \
  erasure-code/jerasure/gf-complete/include/gf_w4.h \
  erasure-code/jerasure/gf-complete/include/gf_w64.h \
  erasure-code/jerasure/gf-complete/include/gf_w8.h \
  erasure-code/jerasure/jerasure/include/galois.h \
  erasure-code/jerasure/jerasure/include/reed_sol.h \
  erasure-code/jerasure/jerasure/include/jerasure.h \
  erasure-code/mlxjerasure/ErasureCodeMlxJerasure.h \
  erasure-code/mlxjerasure/ErasureCodePluginMlxJerasure.h \
  erasure-code/mlxjerasure/ErasureCode_MlxJerasure_common.h 

mlxjerasure_sources = \
  erasure-code/ErasureCode.cc \
  erasure-code/jerasure/jerasure/src/galois.c \
  erasure-code/jerasure/jerasure/src/reed_sol.c \
  erasure-code/jerasure/jerasure/src/jerasure.c \
  erasure-code/jerasure/gf-complete/src/gf_wgen.c \
  erasure-code/jerasure/gf-complete/src/gf_method.c \
  erasure-code/jerasure/gf-complete/src/gf_w16.c \
  erasure-code/jerasure/gf-complete/src/gf.c \
  erasure-code/jerasure/gf-complete/src/gf_w32.c \
  erasure-code/jerasure/gf-complete/src/gf_w64.c \
  erasure-code/jerasure/gf-complete/src/gf_w128.c \
  erasure-code/jerasure/gf-complete/src/gf_general.c \
  erasure-code/jerasure/gf-complete/src/gf_w4.c \
  erasure-code/jerasure/gf-complete/src/gf_rand.c \
  erasure-code/jerasure/gf-complete/src/gf_w8.c \
  erasure-code/mlxjerasure/ErasureCodeMlxJerasure.cc \
  erasure-code/mlxjerasure/ErasureCodePluginMlxJerasure.cc \
  erasure-code/mlxjerasure/ErasureCode_MlxJerasure_common.cc

erasure-code/mlxjerasure/ErasureCodePluginMlxJerasure.cc: ./ceph_ver.h

noinst_LTLIBRARIES += libmlxjerasure.la
libmlxjerasure_la_SOURCES = ${mlxjerasure_sources}
libmlxjerasure_la_CFLAGS = ${AM_CFLAGS}  \
        -I$(srcdir)/erasure-code/jerasure/gf-complete/include \
        -I$(srcdir)/erasure-code/jerasure/jerasure/include
libmlxjerasure_la_CXXFLAGS= ${AM_CXXFLAGS} \
        -I$(srcdir)/erasure-code/jerasure/gf-complete/include \
        -I$(srcdir)/erasure-code/jerasure/jerasure/include
lib_mlxjerasure_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS) -libverbs
lib_mlxjerasure_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
lib_mlxjerasure_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

libec_mlxjerasure_la_SOURCES  = 
libec_mlxjerasure_la_LIBADD = libmlxjerasure.la $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS) -libverbs
libec_mlxjerasure_la_LDFLAGS = ${AM_LDFLAGS} -module -avoid-version -shared
if LINUX
libec_mlxjerasure_la_LDFLAGS += -export-symbols-regex '.*__erasure_code_.*'
endif

erasure_codelib_LTLIBRARIES += libec_mlxjerasure.la


