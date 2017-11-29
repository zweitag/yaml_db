require File.dirname(__FILE__) + '/base'
require 'date'

describe JsonDb::Dump do

	before do
		silence_warnings { ActiveRecord::Base = mock('ActiveRecord::Base', :null_object => true) }
		ActiveRecord::Base.stub(:connection).and_return(stub('connection').as_null_object)
		ActiveRecord::Base.connection.stub!(:tables).and_return([ 'mytable', 'schema_info', 'schema_migrations' ])
		ActiveRecord::Base.connection.stub!(:columns).with('mytable').and_return([ mock('a', name: 'a', type: :string, sql_type: 'text'), mock('b', name: 'b', type: :string, sql_type: 'text') ])
		ActiveRecord::Base.connection.stub!(:select_one).and_return({"count"=>"2"})
		ActiveRecord::Base.connection.stub!(:select_all).and_return([ { 'a' => 1, 'b' => 2 }, { 'a' => 3, 'b' => 4 } ])
	end

	before(:each) do   
	  File.stub!(:new).with('dump.json', 'w').and_return(StringIO.new)
	  @io = StringIO.new
	end

  it "should dump a valid json document with correct data" do
		ActiveRecord::Base.connection.stub!(:columns).and_return([ mock('a', name: 'a', type: :string, sql_type: 'text'), mock('b', name: 'b', type: :string, sql_type: 'text') ])

    JsonDb::Dump.dump(@io)
    @io.rewind
    expect { @json = JSON.load @io }.not_to raise_error

    @json['mytable']['columns'].count.should == 2
    @json['mytable']['records'].should match_array [[1, 2], [3, 4]]
  end

	it "should return a formatted string" do
		JsonDb::Dump.table_record_header(@io)
		@io.rewind
		@io.read.should == '"records": [ '
	end

	it "should return a json string that contains column names" do
		JsonDb::Dump.stub!(:table_column_names).with('mytable').and_return([ 'a', 'b' ])
		JsonDb::Dump.dump_table_columns(@io, 'mytable')
		@io.rewind
    @io.read.should == '"columns": ["a","b"], '
  end

	it "should return dump the records for a table in json to a given io stream" do
		JsonDb::Dump.dump_table_records(@io, 'mytable')
		@io.rewind
		@io.read.should == '"records": [ [1,2],[3,4] ]'
  end

  it 'should dump a valid json document for more than 1000 records' do
		ActiveRecord::Base.connection.stub!(:tables).and_return(['mytable'])
		ActiveRecord::Base.connection.stub!(:select_one).and_return({"count"=>"1001"})
		ActiveRecord::Base.connection.stub!(:select_all).and_return([{'a'=>1, 'b'=>2}] * 1000, [{'a'=>1, 'b'=>2}])

    JsonDb::Dump.dump(@io)
    @io.rewind
    expect { @json = JSON.load @io }.not_to raise_error

    @json['mytable']['columns'].count.should == 2
    @json['mytable']['records'].count.should == 1001
    @json['mytable']['records'].should match_array([[1, 2]] * 1001)
  end

  it 'should dump datetime objects using custom Ruby serialization' do
		ActiveRecord::Base.connection.stub!(:columns).with('mytable').and_return([ mock('datetime', name: 'datetime', type: :datetime, sql_type: 'datetime')])
		ActiveRecord::Base.connection.stub!(:select_one).and_return({"count"=>"1"})
		ActiveRecord::Base.connection.stub!(:select_all).and_return([ { 'datetime' => DateTime.new(2014, 1, 1, 12, 20, 00) } ])
		JsonDb::Dump.dump_table_records(@io, 'mytable')
		@io.rewind
		@io.read.should == '"records": [ [{"json_class":"DateTime","y":2014,"m":1,"d":1,"H":12,"M":20,"S":0,"of":"0/1","sg":2299161.0}] ]'
  end
end
