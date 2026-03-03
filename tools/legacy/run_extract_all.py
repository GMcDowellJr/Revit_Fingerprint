#!/usr/bin/env python3
import runpy
import sys
print('[DEPRECATED] tools/run_extract_all.py moved to tools/governance/run_pipeline.py', file=sys.stderr)
runpy.run_path('tools/governance/run_pipeline.py', run_name='__main__')
