#### requirements

Java 11 runtime or later

#### base

```bash
git clone https://github.com/up-to-you/map-reduce-dummy.git
cd script/bin
# follow instructions
./map-reduce-dummy
```
#### common usage lifecycle
```bash
./map-reduce-dummy generate /home/$(whoami)/huge-file-dir/huge-file
./map-reduce-dummy count /home/$(whoami)/huge-file-dir/huge-file $(python -c "print 'polycarboxylic'*10")
./map-reduce-dummy sort /home/$(whoami)/huge-file-dir/huge-file
```
