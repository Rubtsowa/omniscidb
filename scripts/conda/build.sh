#!/usr/bin/env bash

set -ex

# Free some disk space, see also
# https://github.com/conda-forge/omniscidb-feedstock/issues/5
df -h

export EXTRA_CMAKE_OPTIONS=""

# Make sure -fPIC is not in CXXFLAGS (that some conda packages may
# add), otherwise omniscidb server will crash when executing generated
# machine code:
export CXXFLAGS="`echo $CXXFLAGS | sed 's/-fPIC//'`"

# Fixes https://github.com/Quansight/pearu-sandbox/issues/7
#       https://github.com/omnisci/omniscidb/issues/374
export CXXFLAGS="$CXXFLAGS -Dsecure_getenv=getenv"

# Fixes `error: expected ')' before 'PRIxPTR'`
export CXXFLAGS="$CXXFLAGS -D__STDC_FORMAT_MACROS"

# Remove --as-needed to resolve undefined reference to `__vdso_clock_gettime@GLIBC_PRIVATE'
export LDFLAGS="`echo $LDFLAGS | sed 's/-Wl,--as-needed//'`"

export EXTRA_CMAKE_OPTIONS="$EXTRA_CMAKE_OPTIONS -DCMAKE_C_COMPILER=${CC} -DCMAKE_CXX_COMPILER=${CXX}"

# Run tests labels:
#   0 - disable building and running sanity tests
#   1 - build and run the sanity tests
#   2 - detect if sanity tests can be run, then set 1, otherwise set 0
#
# Ideally, this should 2, but to save disk space, running sanity tests
# will be disabled:
export RUN_TESTS=0

if [[ "$RUN_TESTS" == "0" ]]
then
   export EXTRA_CMAKE_OPTIONS="$EXTRA_CMAKE_OPTIONS -DENABLE_TESTS=off"
else
   export RUN_TESTS=1
   export EXTRA_CMAKE_OPTIONS="$EXTRA_CMAKE_OPTIONS -DENABLE_TESTS=on"
fi

export EXTRA_CMAKE_OPTIONS="$EXTRA_CMAKE_OPTIONS -DBoost_NO_BOOST_CMAKE=on"

#conda activate omnisci-dev-37


. ${RECIPE_DIR}/get_cxx_include_path.sh
export CPLUS_INCLUDE_PATH=$(get_cxx_include_path)

mkdir -p build
cd build

#pip install "pyarrow==0.16"

cmake -Wno-dev \
    -DCMAKE_PREFIX_PATH=$PREFIX \
    -DCMAKE_BUILD_TYPE=release \
    -DMAPD_DOCS_DOWNLOAD=off \
    -DENABLE_AWS_S3=off \
    -DENABLE_FOLLY=off \
    -DENABLE_JAVA_REMOTE_DEBUG=off \
    -DENABLE_PROFILER=off \
    -DPREFER_STATIC_LIBS=off \
    -DENABLE_CUDA=off\
    -DENABLE_DBE=ON \
    -DENABLE_FSI=ON \
    $EXTRA_CMAKE_OPTIONS \
    ..

make -j $CPU_COUNT


if [[ "$RUN_TESTS" == "2" ]]
then
    # Omnisci UDF support uses CLangTool for parsing Load-time UDF C++
    # code to AST. If the C++ code uses C++ std headers, we need to
    # specify the locations of include directories:
    . ${RECIPE_DIR}/get_cxx_include_path.sh
    export CPLUS_INCLUDE_PATH=$(get_cxx_include_path)

    mkdir tmp
    $PREFIX/bin/initdb tmp
    make sanity_tests
    rm -rf tmp
else
    echo "Skipping sanity tests"
fi

make install DESTDIR=$PREFIX

#conda deactivate