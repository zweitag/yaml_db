require File.dirname(__FILE__) + '/base'

describe CsvDb::Dump do

	before do
		silence_warnings { ActiveRecord::Base = mock('ActiveRecord::Base', :null_object => true) }
		ActiveRecord::Base.stub(:connection).and_return(stub('connection').as_null_object)
		ActiveRecord::Base.connection.stub!(:tables).and_return([ 'mytable', 'schema_info', 'schema_migrations' ])
		ActiveRecord::Base.connection.stub!(:columns).with('mytable').and_return([ mock('a', name: 'a', type: :string, sql_type: 'text'), mock('b', name: 'b', type: :string, sql_type: 'text') ])
		ActiveRecord::Base.connection.stub!(:select_one).and_return({"count"=>"2"})
		ActiveRecord::Base.connection.stub!(:select_all).and_return([ { 'a' => 1, 'b' => 2 }, { 'a' => 3, 'b' => 4 } ])
	end

	before(:each) do
	  File.stub!(:new).with('dump.csv', 'w').and_return(StringIO.new)
	  @io = StringIO.new
	end

	it "should return a csv string that contains a table header and column names" do
		CsvDb::Dump.stub!(:table_column_names).with('mytable').and_return([ 'a', 'b' ])
		CsvDb::Dump.dump_table_columns(@io, 'mytable')
		@io.rewind
    expected_csv = <<EOCSV
a,b
EOCSV
    expected_csv.gsub!(" \n", "\n") if RUBY_VERSION.to_f >= 2.0
		@io.read.should == expected_csv
  end

	it "should return dump the records for a table in csv to a given io stream" do
		CsvDb::Dump.dump_table_records(@io, 'mytable')
		@io.rewind
		@io.read.should == <<EOCSV
1,2
3,4
EOCSV
	end
end
