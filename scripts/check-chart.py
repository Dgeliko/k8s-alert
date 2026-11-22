#!/usr/bin/env python3
"""Check behavior of the rendered chart, using Helm and Python stdlib only."""
import subprocess

def render(*args, ok=True):
    p = subprocess.run(['helm', 'template', 'check', 'charts/k8s-alert', '-n', 'test', *args], capture_output=True, text=True)
    assert (p.returncode == 0) == ok, p.stderr
    return p.stdout

s = render()
assert 'secretName: k8s-alert-telegram' not in s, 'disabled Telegram mounted'
deployment = next(doc for doc in s.split('---') if '\nkind: Deployment\n' in doc)
metadata, spec = deployment.split('\nspec:\n', 1)
assert '    reloader.stakater.com/auto: "true"' in metadata
assert 'check-state' not in spec, 'state must never trigger auto-reload'
assert 'checksum/config:' not in s
assert 'resourceNames: ["check-state"]' in s
assert 'replicas: 1' in s and 'type: Recreate' in s
s = render('--set', 'reloader.enabled=false')
assert 'reloader.stakater.com/auto:' not in s
assert 'checksum/config:' in s
s = render('-f', 'examples/telegram.yaml')
assert 'secretName: k8s-alert-telegram' in s
render('--set', 'replicaCount=2', ok=False)
print('Helm behavior checks passed')
