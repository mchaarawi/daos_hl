#!python

import os
import sys

##sys.path.insert(0, "./scons_local")
##from prereq_tools import *

ROOT = Dir('#').abspath
DAOS_HL_VERSION = "0.0.1"
SRC_DIRS = ['array',
            '.',
           ]

def save_build_info(env, prereqs, platform):
    """Save the build information"""

    build_info = prereqs.get_build_info()

    #Save the build info locally
    json_build_vars = '.build_vars-%s.json' % platform
    sh_build_vars = '.build_vars-%s.sh' % platform
    build_info.save(json_build_vars)
    build_info.gen_script(sh_build_vars)

    #Install the build info to the testing directory
    env.InstallAs('$PREFIX/TESTING/.build_vars.sh',
                  sh_build_vars)
    env.InstallAs('$PREFIX/TESTING/.build_vars.json',
                  json_build_vars)

def load_sconscripts(arch_dir):
    """Load the SConscripts"""
    VariantDir(arch_dir, '.', duplicate=0)
    src_prefix = '%s/src' % arch_dir
    for src_dir in SRC_DIRS:
        SConscript('%s/%s/SConscript' % (src_prefix, src_dir))
        Default('src/%s' % src_dir)
##    SConscript('%s/scons_local/test_runner/SConscript' % arch_dir)
##    Default('example')

def scons():
    """Run SCons"""

    platform = os.uname()[0]
    arch_dir = 'build/%s' % platform
    opt_file = os.path.join(ROOT, "daos_hl-%s.conf" % platform)

    if os.path.exists('daos_hl.conf') and not os.path.exists(opt_file):
        print 'Renaming legacy conf file'
        os.rename('daos_hl.conf', opt_file)

    opts = Variables(opt_file)

    AddOption('--prefix',
              dest='prefix',
              type='string',
              nargs=1,
              action='store',
              metavar='DIR',
              help='installation prefix')

    env = Environment(PREFIX = GetOption('prefix'))

    print "PREFIX is", env['PREFIX']
    INCLUDE_PREFIX = os.path.join("$PREFIX", "include")
    LIB_PREFIX = os.path.join("$PREFIX", "lib")
    BIN_PREFIX = os.path.join("$PREFIX", "bin")

##    env = DefaultEnvironment()
#    env = Environment()

    config = Configure(env)

#    if not config.CheckHeader("daos_api.h"):
#        print "daos_api.h header is required"
#        Exit(-1)
#    if not config.CheckHeader("daos_types.h"):
#        print "daos_types.h header is required"
#        Exit(-1)
#    if not config.CheckLib("daos"):
#        print "libdaos package is required"
#        Exit(-1)

    env.Append(CCFLAGS=['-g', '-Wall', '-Werror', '-D_GNU_SOURCE', '-fPIC'])
    env.Append(CPPPATH=['/home/mschaara/source/daos_m/src/include'])
    env.Append(LIBS=['daos', 'uuid', 'crt'])
    env.Append(LIBPATH=['/home/mschaara/build/daos_m/build/lib'])

    Export('env INCLUDE_PREFIX LIB_PREFIX BIN_PREFIX DAOS_HL_VERSION')
    config.Finish()

    load_sconscripts(arch_dir)
    print "HERE"
    env.Alias('install', '$PREFIX')

    try:
        Help(opts.GenerateHelpText(env), append=True)
    except TypeError:
        Help(opts.GenerateHelpText(env))

if __name__ == 'SCons.Script':
    scons()
