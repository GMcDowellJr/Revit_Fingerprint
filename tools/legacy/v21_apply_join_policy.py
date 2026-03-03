#!/usr/bin/env python3
import runpy
import sys
print('[DEPRECATED] tools/v21_apply_join_policy.py moved to tools/policy/apply_join_policy.py', file=sys.stderr)
runpy.run_path('tools/policy/apply_join_policy.py', run_name='__main__')
