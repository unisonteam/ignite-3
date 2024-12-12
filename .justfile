dist_name := 'ignite3-3.0.0-SNAPSHOT'
dist_db_name := 'ignite3-db-3.0.0-SNAPSHOT'
dist_cli_name := 'ignite3-cli-3.0.0-SNAPSHOT'

cli_dir := 'ignite3-cli'
node_name_prefix := 'ignite3-db'

_change_node_name idx:
    sed -i '' 's/NODE_NAME=defaultNode/NODE_NAME=node{{idx}}/' w/{{node_name_prefix}}-{{idx}}/etc/vars.env

_increment_ports idx:
  sed -i '' 's/port=10300/port=1030{{idx}}/' w/{{node_name_prefix}}-{{idx}}/etc/ignite-config.conf
  sed -i '' 's/port=3344/port=330{{idx}}/' w/{{node_name_prefix}}-{{idx}}/etc/ignite-config.conf
  sed -i '' 's/port=10800/port=1080{{idx}}/' w/{{node_name_prefix}}-{{idx}}/etc/ignite-config.conf

  sed -i '' '/netClusterNodes=\[/,/\]/s/"localhost:3344"/"localhost:3301"/' w/{{node_name_prefix}}-{{idx}}/etc/ignite-config.conf

_cp_db idx:
  cp -r w/{{dist_db_name}} w/{{node_name_prefix}}-{{idx}}

cli:
  w/{{cli_dir}}/bin/ignite3

start idx:
  w/{{node_name_prefix}}-{{idx}}/bin/ignite3db start

clean:
  rm -rf w
  ./gradlew clean

init:
  rm -rf w

  mkdir w

  ./gradlew allDistZip

  unzip packaging/build/distributions/{{dist_name}}.zip -d w

  just _cp_db 1
  just _cp_db 2
  just _cp_db 3

  mv w/{{dist_cli_name}} w/{{cli_dir}}

  just _change_node_name 1
  just _change_node_name 2
  just _change_node_name 3

  just _increment_ports 1
  just _increment_ports 2
  just _increment_ports 3

  rm -rf w/{{dist_db_name}}


