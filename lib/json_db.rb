require 'rubygems'
require 'json'
require 'active_record'
require 'serialization_helper'

module JsonDb
  module Helper
    def self.loader
      Load
    end

    def self.dumper
      Dump
    end

    def self.extension
      "json"
    end
  end

  class Dump < SerializationHelper::Dump
    def self.dump(io)
      io.write '{'
      super io
      io.write '}'
    end

    def self.before_table(io, table)
      io.write '"' + table + '": {'
    end

    def self.after_table(io, table)
      if table == tables.last
        io.write "}\n"
      else
        io.write "},\n"
      end
    end

    def self.dump_table_columns(io, table)
      io.write '"columns": ' + JSON.dump(table_column_names(table)) + ', '
    end

    def self.table_record_header(io)
      io.write('"records": [ ')
    end

    def self.dump_table_records(io, table)
      table_record_header(io)

      column_names = table_column_names(table)

      each_table_page(table) do |records|
        rows = SerializationHelper::Utils.unhash_records(records, column_names)
        io.write JSON.dump(rows)[1..-2]      # without opening and closing brackets
      end

      io.write ' ]'
    end
  end

  class Load < SerializationHelper::Load
    def self.load_documents(io, truncate = true)
        JSON.load(io).tap do |json|
          json.keys.each do |table_name|
            next if json[table_name].nil?
            load_table(table_name, json[table_name], truncate)
          end
        end
    end
  end
end
