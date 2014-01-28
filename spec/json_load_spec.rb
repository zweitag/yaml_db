require File.dirname(__FILE__) + '/base'
require 'active_support/core_ext/kernel/debugger'

describe JsonDb::Load do
	before do
		SerializationHelper::Utils.stub!(:quote_table).with('mytable').and_return('mytable')

		silence_warnings { ActiveRecord::Base = mock('ActiveRecord::Base', :null_object => true) }
		ActiveRecord::Base.stub(:connection).and_return(stub('connection').as_null_object)
		ActiveRecord::Base.connection.stub!(:transaction).and_yield
	end

	before(:each) do
		@io = StringIO.new
    end
    

	it "should call load_table for each table in the file" do
		JSON.should_receive(:load).with(@io).and_return({ 'mytable' => { 
					'columns' => [ 'a', 'b' ], 
					'records' => [[1, 2], [3, 4]] 
				} } )
		JsonDb::Load.should_receive(:load_table).with('mytable', { 'columns' => [ 'a', 'b' ], 'records' => [[1, 2], [3, 4]] },true)
		JsonDb::Load.load(@io)
	end

	it "should not call load_table when the table in the file contains no records" do
		JSON.should_receive(:load).with(@io).and_return({ 'mytable' => nil })
		JsonDb::Load.should_not_receive(:load_table)
		JsonDb::Load.load(@io)
	end

end
