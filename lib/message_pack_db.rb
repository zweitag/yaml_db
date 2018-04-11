require 'rubygems'
require 'msgpack'
require 'active_record'
require 'serialization_helper'

module MessagePackDb
  def self.factory
    @factory ||= MessagePack::Factory.new.tap do |f|
      f.register_type(0x01, DateTime, packer: :to_s, unpacker: DateTime.method(:parse))
      f.register_type(0x02, Time, packer: :to_s, unpacker: Time.method(:parse))
      f.register_type(0x03, Date, packer: :to_s, unpacker: Date.method(:parse))
      f.register_type(0x04, BigDecimal, packer: :to_s, unpacker: BigDecimal.method(:new))
      f.register_type(0x20, TableHeader)
    end
  end

  module Helper
    def self.loader
      Load
    end

    def self.dumper
      Dump
    end

    def self.extension
      "mpk"
    end
  end

  class TableHeader
    def self.from_msgpack_ext(mpk)
      args = MessagePack.unpack(mpk)
      new name: args[0], columns: args[1], count: args[2]
    end

    attr_reader :name, :columns, :count
    def initialize(name:, columns:, count:)
      @name, @columns, @count = name, columns, count
    end

    def to_msgpack_ext(*args)
      [name, columns, count].to_msgpack(*args)
    end
  end

  class Dump < SerializationHelper::Dump
    def self.dump(io)
      packer = MessagePackDb.factory.packer

      tables.each do |table|
        dump_table(packer, table)

        packer.write_to(io)
      end
    end

    def self.dump_table(packer, table)
      return if table_record_count(table).zero?

      dump_table_header(packer, table)
      dump_table_records(packer, table)
    end

    def self.dump_table_header(packer, table)
      header = TableHeader.new(
        name: table,
        columns: table_column_names(table),
        count: table_record_count(table)
      )
      packer.write(header)
    end

    def self.dump_table_records(packer, table)
      column_names = table_column_names(table)

      each_table_page(table) do |records, page_types|
        rows = SerializationHelper::Utils.unhash_records(records, column_names)
        packer.write(rows)
      end
    end
  end

  class Load < SerializationHelper::Load
    def self.load_documents(io)
      unpacker = MessagePackDb.factory.unpacker(io)

      header = nil
      unpacker.each do |object|
        if object.is_a?(TableHeader)
          header = object
        else
          raise "header missing" if header.nil?

          load_table header.name, {'columns' => header.columns, 'records' => object}
        end
      end
    end
  end
end
