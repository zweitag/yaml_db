module SerializationHelper

  class Base
    attr_reader :extension

    def initialize(helper)
      @dumper = helper.dumper
      @loader = helper.loader
      @extension = helper.extension
    end

    def dump(filename)
      disable_logger
      @dumper.dump(File.new(filename, "wb"))
      reenable_logger
    end

    def dump_to_dir(dirname)
      Dir.mkdir(dirname)
      tables = @dumper.tables
      tables.each do |table|
        io = File.new "#{dirname}/#{table}.#{@extension}", "w"
        @dumper.before_table(io, table)
        @dumper.dump_table io, table
        @dumper.after_table(io, table)
      end
    end

    def dump_to_io(io)
      disable_logger
      @dumper.dump(io)
      reenable_logger
    end

    def load(filename, truncate = true)
      disable_logger
      @loader.load(File.new(filename, "r"), truncate)
      reenable_logger
    end

    def load_from_dir(dirname, truncate = true)
      Dir.entries(dirname).each do |filename|
        if filename =~ /^[.]/
          next
        end
        @loader.load(File.new("#{dirname}/#{filename}", "r"), truncate)
      end
    end

    def load_from_io(io, truncate = true)
      disable_logger
      @loader.load(io, truncate)
      reenable_logger
    end

    def disable_logger
      @@old_logger = ActiveRecord::Base.logger
      ActiveRecord::Base.logger = nil
    end

    def reenable_logger
      ActiveRecord::Base.logger = @@old_logger
    end
  end

  class Load
    def self.load(io, truncate = true)
      ActiveRecord::Base.connection.transaction do
        defer_fk_constraints do
          truncate_all if truncate
          load_documents(io)
        end
      end
    end

    def self.truncate_all
      quoted_tables = tables.map do |table|
        SerializationHelper::Utils.quote_table(table)
      end
      case database_type
      when :postgresql
        ActiveRecord::Base.connection.execute("TRUNCATE #{quoted_tables.join(',')} CASCADE")
      when :mysql
        quoted_tables.each do |quoted_table|
          ActiveRecord::Base.connection.execute("TRUNCATE #{quoted_table}")
        end
      end
    end

    def self.tables
      ActiveRecord::Base.connection.tables
    end


    def self.load_table(table, data)
      return if table == 'ar_internal_metadata'
      column_names = data['columns']
      load_records(table, column_names, data['records'])
      reset_pk_sequence!(table)
    end

    def self.load_records(table, column_names, records, records_per_page=1000)
      if column_names.nil?
        return
      end
      columns = column_names.map{|cn| ActiveRecord::Base.connection.columns(table).detect{|c| c.name == cn}}
      quoted_column_names = column_names.map { |column| ActiveRecord::Base.connection.quote_column_name(column) }.join(',')
      quoted_table_name = SerializationHelper::Utils.quote_table(table)

      0.step(records.count-1, records_per_page) do |offset|
        all_quoted_values = records[offset, records_per_page].map do |record|
          '(' + record.zip(columns).map{|c| ActiveRecord::Base.connection.quote(c.first, c.last)}.join(',') + ')'
        end.join(', ')
        ActiveRecord::Base.connection.execute("INSERT INTO #{quoted_table_name} (#{quoted_column_names}) VALUES #{all_quoted_values}")
      end
    end

    def self.reset_pk_sequence!(table_name)
      if ActiveRecord::Base.connection.respond_to?(:reset_pk_sequence!)
        ActiveRecord::Base.connection.reset_pk_sequence!(table_name)
      end
    end

    def self.defer_fk_constraints(&block)
      case database_type
      when :postgresql
        # make all fk constraints deferrable
        tables.each do |table|
          fk_constraints_on_table = ActiveRecord::Base.connection.foreign_keys(table)
          fk_constraints_on_table.each do |fk_constraint|
            quoted_table_name = SerializationHelper::Utils.quote_table(table)
            ActiveRecord::Base.connection.execute("ALTER TABLE #{quoted_table_name} ALTER CONSTRAINT #{fk_constraint.name} DEFERRABLE INITIALLY IMMEDIATE")
          end
        end
        # defer all fk constraints
        ActiveRecord::Base.connection.execute("SET CONSTRAINTS ALL DEFERRED")
        yield block
      when :mysql
        ActiveRecord::Base.connection.execute("SET foreign_key_checks = 0")
        yield block
        ActiveRecord::Base.connection.execute("SET foreign_key_checks = 1")
      else
        # for testing purposes
        yield block
      end
    end

    def self.database_type
      case ActiveRecord::Base.connection.class.name
      when 'ActiveRecord::ConnectionAdapters::PostgreSQLAdapter'
        :postgresql
      when 'ActiveRecord::ConnectionAdapters::Mysql2Adapter', 'ActiveRecord::ConnectionAdapters::MysqlAdapter'
        :mysql
      end
    end
  end

  module Utils

    def self.unhash_records(records, keys)
      records.map do |record|
        keys.map { |key| record[key] }
      end
    end

    def self.convert_booleans(records, columns)
      records.map do |record|
        columns.each do |column|
          next if is_boolean(record[column])
          record[column] = convert_boolean(record[column])
        end
        record
      end
    end

    def self.convert_jsons(records, columns)
      records.map do |record|
        columns.each do |column|
          next if is_json(record[column])
          record[column] = convert_json(record[column])
        end
        record
      end
    end

    def self.convert_boolean(value)
      ['t', '1', true, 1].include?(value)
    end

    def self.convert_json(value)
      return nil if value.nil?
      JSON.parse(value)
    end

    def self.boolean_columns(table)
      columns = ActiveRecord::Base.connection.columns(table).reject { |c| silence_warnings { c.type != :boolean } }
      columns.map { |c| c.name }
    end

    def self.json_columns(table)
      columns = ActiveRecord::Base.connection.columns(table).select { |c| c.sql_type == 'json' }
      columns.map { |c| c.name }
    end

    def self.is_boolean(value)
      value.kind_of?(TrueClass) or value.kind_of?(FalseClass)
    end

    def self.is_json(value)
      value.kind_of?(Hash) or value.kind_of?(Array)
    end

    def self.quote_table(table)
      ActiveRecord::Base.connection.quote_table_name(table)
    end

  end

  class Dump
    def self.before_table(io, table)

    end

    def self.dump(io)
      tables.each do |table|
        before_table(io, table)
        dump_table(io, table)
        after_table(io, table)
      end
    end

    def self.after_table(io, table)

    end

    def self.tables
      ActiveRecord::Base.connection.tables
    end

    def self.dump_table(io, table)
      return if table_record_count(table).zero?

      dump_table_columns(io, table)
      dump_table_records(io, table)
    end

    def self.table_column_names(table)
      ActiveRecord::Base.connection.columns(table).map { |c| c.name }
    end


    def self.each_table_page(table, records_per_page=1000)
      total_count = table_record_count(table)
      pages = (total_count.to_f / records_per_page).ceil - 1
      keys = sort_key(table)
      boolean_columns = SerializationHelper::Utils.boolean_columns(table)
      json_columns = SerializationHelper::Utils.json_columns(table)
      quoted_table_name = SerializationHelper::Utils.quote_table(table)

      (0..pages).to_a.each_with_index do |page, index|
        query = Arel::Table.new(table).order(*keys).skip(records_per_page*page).take(records_per_page).project(Arel.sql('*'))
        records = ActiveRecord::Base.connection.select_all(query)
        records = SerializationHelper::Utils.convert_booleans(records, boolean_columns)
        records = SerializationHelper::Utils.convert_jsons(records, json_columns)

        page_types = []
        page_types << :first if index == 0
        page_types << :last if index == pages

        yield records, page_types
      end
    end

    def self.table_record_count(table)
      ActiveRecord::Base.connection.select_one("SELECT COUNT(*) FROM #{SerializationHelper::Utils.quote_table(table)}").values.first.to_i
    end

    # Just return the first column as sort key unless the table looks like a
    # standard HABTM join table, in which case add the second "id column"
    def self.sort_key(table)
      first_column, second_column = table_column_names(table)

      if [first_column, second_column].all? { |name| name =~ /_id$/ }
        [first_column, second_column]
      else
        first_column
      end
    end
  end

end
