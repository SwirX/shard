# shard service units

`shard daemon` is meant to run as a lean background service. Two process
supervisors are supported.

## systemd (user unit)

```
cp packaging/shard.service  ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now shard.service
```

Requires `shard` installed at `~/.local/bin/shard` (e.g. via
`cargo install --path crates/shard-cli --root ~/.local`).

## runit

```
mkdir -p ~/.local/share/runit/shard
cp packaging/runit/shard/run ~/.local/share/runit/shard/
chmod +x ~/.local/share/runit/shard/run
runsvdir ~/.local/share/runit &
```

Point your session startup at `runsvdir ~/.local/share/runit` (the
`shard` directory is the service). `sv t ~/.local/share/runit/shard`
restarts it; the run script uses `chpst` to drop privileges — replace
with a plain `exec "$HOME/.local/bin/shard" daemon` if unavailable.