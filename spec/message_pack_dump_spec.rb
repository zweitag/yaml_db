require File.dirname(__FILE__) + '/base'
require 'date'

describe MessagePackDb::Dump do
  let(:connection) { double('connection') }

	before do
		allow(ActiveRecord::Base).to receive(:connection).and_return(connection)
		allow(connection).to receive(:tables).and_return([ 'mytable', 'schema_info', 'schema_migrations' ])
		allow(connection).to receive(:columns).with('mytable').and_return([ double('a', name: 'a', type: :string, sql_type: 'text'), double('b', name: 'b', type: :string, sql_type: 'text') ])
		allow(connection).to receive(:select_one).and_return({"count"=>"2"})
		allow(connection).to receive(:select_all).and_return([ { 'a' => 1, 'b' => 2 }, { 'a' => 3, 'b' => 4 } ])
		allow(connection).to receive(:quote_table_name) {|table| table }
	end

	before(:each) do
	  @io = StringIO.new
	end

  it "should dump a valid messagepack document with correct data" do
		allow(connection).to receive(:columns).and_return([ double('a', name: 'a', type: :string, sql_type: 'text'), double('b', name: 'b', type: :string, sql_type: 'text') ])

    MessagePackDb::Dump.dump(@io)
    @io.rewind
    tables = []
    records = {}
    unpacker = MessagePackDb.factory.unpacker(@io)
    expect do
      is_header = true
      unpacker.each do |item|
        if is_header
          tables << item
        else
          (records[tables.last.name] ||= []).push(*item)
        end

        is_header = !is_header
      end
    end.not_to raise_error
    expect { unpacker.read }.to raise_error(EOFError)

    expect(tables.count).to eq 3
    table = tables.first
    expect(table.name).to eq('mytable')
    expect(table.columns).to match_array ['a', 'b']
    expect(records[table.name]).to match_array [[1, 2], [3, 4]]
  end

  it 'should dump valid messagepack data for more than 1000 records' do
    allow(connection).to receive(:tables).and_return(['mytable'])
    allow(connection).to receive(:select_one).and_return({"count"=>"1001"})
    allow(connection).to receive(:select_all).and_return([{'a'=>1, 'b'=>2}] * 1000, [{'a'=>1, 'b'=>2}])

    MessagePackDb::Dump.dump(@io)
    @io.rewind
    unpacker = MessagePackDb.factory.unpacker(@io)

    unpacker.read     # table. columns
    records = unpacker.read
    more_records = unpacker.read
    expect { unpacker.read }.to raise_error(EOFError)

    expect(records.count).to eq(1000)
    expect(records).to eq([[1, 2]] * 1000)
    expect(more_records.count).to eq(1)
    expect(more_records).to eq([[1, 2]])
  end

  it 'should dump datetime objects using custom Ruby serialization' do
    allow(connection).to receive(:columns).with('mytable').and_return([ double('datetime', name: 'datetime', type: :datetime, sql_type: 'datetime')])
    allow(connection).to receive(:select_one).and_return({"count"=>"1"})
    allow(connection).to receive(:select_all).and_return([ { 'datetime' => DateTime.new(2014, 1, 1, 12, 20, 00) } ])

    packer = MessagePackDb::factory.packer
    MessagePackDb::Dump.dump_table(packer, 'mytable')
    packer.write_to(@io)
    @io.rewind
    @io.sysread.should include "2014-01-01T12:20:00"
  end

  it 'should dump date objects using custom Ruby serialization' do
    allow(connection).to receive(:columns).with('mytable').and_return([ double('date', name: 'date', type: :date, sql_type: 'date')])
    allow(connection).to receive(:select_one).and_return({"count"=>"1"})
    allow(connection).to receive(:select_all).and_return([ { 'date' => Date.new(2014, 1, 1) } ])

    packer = MessagePackDb::factory.packer
    MessagePackDb::Dump.dump_table(packer, 'mytable')
    packer.write_to(@io)
    @io.rewind
    @io.sysread.should include "2014-01-01"
  end

  it 'should dump time objects using custom Ruby serialization' do
    allow(connection).to receive(:columns).with('mytable').and_return([ double('time', name: 'time', type: :time, sql_type: 'time')])
    allow(connection).to receive(:select_one).and_return({"count"=>"1"})
    allow(connection).to receive(:select_all).and_return([ { 'time' => Time.new(2014, 1, 1, 12, 20, 00) } ])

    packer = MessagePackDb::factory.packer
    MessagePackDb::Dump.dump_table(packer, 'mytable')
    packer.write_to(@io)
    @io.rewind
    @io.sysread.should include "2014-01-01 12:20:00 +0000"
  end

  it 'should dump BigDecimal objects using custom Ruby serialization' do
    allow(connection).to receive(:columns).with('mytable').and_return([ double('bigdecimal', name: 'bigdecimal', type: :big_decimal, sql_type: 'double')])
    allow(connection).to receive(:select_one).and_return({"count"=>"1"})
    allow(connection).to receive(:select_all).and_return([ { 'bigdecimal' => BigDecimal.new('1234.56') } ])

    packer = MessagePackDb::factory.packer
    MessagePackDb::Dump.dump_table(packer, 'mytable')
    packer.write_to(@io)
    @io.rewind
    @io.sysread.should include "1234.56"
  end

  #it 'should correctly serialize json columns' do
    #ActiveRecord::Base.connection.stub!(:columns).with('mytable').and_return([ mock('json', name: 'json', type: :json, sql_type: 'json')])
    #ActiveRecord::Base.connection.stub!(:select_one).and_return({"count"=>"1"})
    #ActiveRecord::Base.connection.stub!(:select_all).and_return([ { 'json' => '[{"a":1},{"b":2}]' } ])
    #JsonDb::Dump.dump_table_records(@io, 'mytable')
    #@io.rewind
    #@io.read.should == '"records": [ [[{"a":1},{"b":2}]] ]'
  #end
end
