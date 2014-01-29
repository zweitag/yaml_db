require File.dirname(__FILE__) + '/base'

describe JsonDb::Dump do

	before do
		silence_warnings { ActiveRecord::Base = mock('ActiveRecord::Base', :null_object => true) }
		ActiveRecord::Base.stub(:connection).and_return(stub('connection').as_null_object)
		ActiveRecord::Base.connection.stub!(:tables).and_return([ 'mytable', 'schema_info', 'schema_migrations' ])
		ActiveRecord::Base.connection.stub!(:columns).with('mytable').and_return([ mock('a',:name => 'a', :type => :string), mock('b', :name => 'b', :type => :string) ])
		ActiveRecord::Base.connection.stub!(:select_one).and_return({"count"=>"2"})
		ActiveRecord::Base.connection.stub!(:select_all).and_return([ { 'a' => 1, 'b' => 2 }, { 'a' => 3, 'b' => 4 } ])
	end

	before(:each) do   
	  File.stub!(:new).with('dump.json', 'w').and_return(StringIO.new)
	  @io = StringIO.new
	end

  it "should dump a valid json document with correct data" do
		ActiveRecord::Base.connection.stub!(:columns).and_return([ mock('a',:name => 'a', :type => :string), mock('b', :name => 'b', :type => :string) ])

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
end
