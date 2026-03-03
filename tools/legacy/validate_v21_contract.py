#!/usr/bin/env python3
import runpy
import sys
print('[DEPRECATED] tools/validate_v21_contract.py moved to tools/governance/validate_contract.py', file=sys.stderr)
runpy.run_path('tools/governance/validate_contract.py', run_name='__main__')
