import os


root_dir = os.getcwd() + '/'
build_dir = root_dir + 'build/'
src_dir = root_dir + 'src/'

force_include_headers = []

#===============================================================================
# common defines
#===============================================================================
common_compiler_defines = [
        '-Dforce_inline=__attribute__((always_inline)) inline ',
        '-Dforce_no_inline=__attribute__((noinline)) '
        ]


#===============================================================================
# common compiler flags
#===============================================================================
common_compiler_flags = ['-std=c++11', '-rdynamic', '-pipe', '-Wall', '-Wextra', '-Wno-unused-function', '-fPIC']
common_compiler_flags = common_compiler_flags + force_include_headers


#===============================================================================
# library dependencies
#===============================================================================
common_library_dependencies = ['pthread']
debug_library_dependencies = []
release_library_dependencies = []


#===============================================================================
# library dependency paths
#===============================================================================
common_library_dependency_paths = []
debug_library_dependency_paths = [build_dir + 'debug/lib/']
release_library_dependency_paths = [build_dir + 'release/lib/']


#===============================================================================
# include paths
#===============================================================================
common_include_paths = []
debug_include_paths = common_include_paths + [build_dir + 'debug/src/']
release_include_paths = common_include_paths + [build_dir + 'release/src/']


#===============================================================================
# optional build flags
#===============================================================================
build_executable = ARGUMENTS.get('executable', '1')


#===============================================================================
# common environment for all build configurations
#===============================================================================
common_env = Environment()
common_env.Append(CPPDEFINES = common_compiler_defines)
common_env.Append(CXXCOMSTR = 'compiling [$SOURCE]')
common_env.Append(LINKCOMSTR = 'linking [$TARGET]')
common_env.Append(ARCOMSTR = 'archiving [$TARGET]')
common_env.Append(INSTALLSTR = 'installing [$TARGET]')
common_env.Append(CCFLAGS = common_compiler_flags)
common_env.Append(LIBS = common_library_dependencies)
common_env.Append(LIBPATH = common_library_dependency_paths)
common_env.Clean('.', build_dir)


#===============================================================================
# release build environment
#===============================================================================
release_compiler_flags = ['-g', '-O3', '-march=native', '-fno-stack-protector', '-funroll-loops', '-funsafe-loop-optimizations', '-pthread']
release_compiler_defines = ['RELEASE']
release_env = common_env.Clone()
release_env.Append(CPPDEFINES = release_compiler_defines)
release_env.Append(LIBPATH = release_library_dependency_paths)
release_env.Append(CPPPATH = release_include_paths)
release_env.Append(CCFLAGS = release_compiler_flags)
release_env.Append(LIBS = release_library_dependencies)


#===============================================================================
# debug build environment
#===============================================================================
debug_compiler_flags = ['-g', '-O0', '-pthread']
debug_compiler_defines = ['DEBUG']
debug_env = common_env.Clone()
debug_env.Append(CPPDEFINES = debug_compiler_defines)
debug_env.Append(LIBPATH = debug_library_dependency_paths)
debug_env.Append(CPPPATH = debug_include_paths)
debug_env.Append(CCFLAGS = debug_compiler_flags)
debug_env.Append(LIBS = debug_library_dependencies)


#===============================================================================
# construct list of environments to build.  default is 'all'
#===============================================================================
build_config = ARGUMENTS.get('config', 'all')
env_dict = {}
if build_config == 'release' or build_config == 'all':
    env_dict['release'] = release_env
if build_config == 'debug' or build_config == 'all':
    env_dict['debug'] = debug_env


#===============================================================================
# optional build flags
#===============================================================================
build_executable = ARGUMENTS.get('executable', '1')


#===============================================================================
# build and install all executables
#===============================================================================

for mode, env in env_dict.iteritems():
    build_src_dir = build_dir + '%s/src/' % mode
    install_dir = build_dir + '%s/' % mode
    env.VariantDir(build_src_dir, src_dir)
    env.SConscript(build_src_dir + '/SConscript', exports = {'env' : env, 'install_dir' : install_dir, 'build_executable' : build_executable})

