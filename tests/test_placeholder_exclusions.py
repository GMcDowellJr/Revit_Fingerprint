from pathlib import Path
import csv, subprocess, sys

def _wcsv(p,rows):
    p.parent.mkdir(parents=True,exist_ok=True)
    with p.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0].keys()));w.writeheader();w.writerows(rows)

def test_placeholder_exclusions_smoke(tmp_path):
    phase0=tmp_path/'phase0'; out=tmp_path/'out'; pol=tmp_path/'policies'
    rows=[
      {'domain':'wall_types','file_id':'f1','label_display':'Wall 1','is_purgeable':'true','instance_count':'0','is_sole_type_in_category':'true'},
      {'domain':'line_styles','file_id':'f1','label_display':'<Lines>','is_purgeable':'false','instance_count':'','is_sole_type_in_category':''},
      {'domain':'text_types','file_id':'f2','label_display':'Text 1','is_purgeable':'false','instance_count':'0','is_sole_type_in_category':'true'},
    ]
    _wcsv(phase0/'phase0_records.csv',rows)
    pol.mkdir(parents=True, exist_ok=True)
    (pol/'placeholder_known_defaults.json').write_text(Path('policies/placeholder_known_defaults.json').read_text(),encoding='utf-8')
    subprocess.check_call([sys.executable,'tools/bundle_analysis/placeholder_exclusions.py','--phase0-dir',str(phase0),'--policies-dir',str(pol),'--out-dir',str(out)])
    assert (out/'placeholder_exclusions_wall_types.csv').is_file()
    all_rows=list(csv.DictReader((out/'placeholder_exclusions_all.csv').open()))
    w1=[r for r in all_rows if r['domain']=='wall_types'][0]
    assert w1['suggested_exclude']=='true'
    assert w1['excluded']=='false'
    ls=[r for r in all_rows if r['domain']=='line_styles'][0]
    assert ls['suggested_exclude']=='false'
