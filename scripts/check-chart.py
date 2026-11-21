#!/usr/bin/env python3
"""Check behavior of the rendered chart, using Helm and Python stdlib only."""
import subprocess

def render(*args, ok=True):
    p = subprocess.run(['helm', 'template', 'check', 'charts/k8s-alert', '-n', 'test', *args], capture_output=True, text=True)
    assert (p.returncode == 0) == ok, p.stderr
    return p.stdout

s = render()
assert 'secretName: k8s-alert-telegram' not in s, 'disabled Telegram mounted'
assert 'configmap.reloader.stakater.com/reload: "check-config"' in s
assert 'secret.reloader.stakater.com/reload: "k8s-alert-pachca"' in s
assert 'reloader.stakater.com/auto:' not in s, 'state must never trigger auto-reload'
assert 'checksum/config:' not in s
assert 'resourceNames: ["check-state"]' in s
assert 'replicas: 1' in s and 'type: Recreate' in s
s = render('--set', 'reloader.enabled=false')
assert 'checksum/config:' in s
s = render('-f', 'examples/telegram.yaml')
assert 'secretName: k8s-alert-telegram' in s
render('--set', 'replicaCount=2', ok=False)
print('Helm behavior checks passed')
