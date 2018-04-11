$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rspec'
require 'yaml_db'
require 'json_db'
require 'csv_db'
require 'message_pack_db'
