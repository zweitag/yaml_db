require File.dirname(__FILE__) + '/base'

describe MessagePackDb::Load do
  let(:connection) { double('connection') }
	before do
		allow(ActiveRecord::Base).to receive(:connection).and_return(connection)
		allow(connection).to receive(:transaction).and_yield
		allow(connection).to receive(:tables).and_return([ 'mytable', 'schema_info', 'schema_migrations' ])
		allow(connection).to receive(:quote_table_name) {|table| table }
	end

	before(:each) do
		@io = StringIO.new
  end

  it 'should correctly deserialize the data' do
    # mytable with two separate record objects (each w/ 2 records)
    io = StringIO.new "\xC7\x0F \x93\xA7mytable\x92\xA1a\xA1b\x02\x92\x92\x01\x02\x92\x03\x04\xC7\x13 \x93\xABschema_info\x92\xA1a\xA1b\x02\x92\x92\x01\x02\x92\x03\x04\xC7\x19 \x93\xB1schema_migrations\x92\xA1a\xA1b\x02\x92\x92\x01\x02\x92\x03\x04"

		expect(MessagePackDb::Load).to receive(:load_table).once.with('mytable', { 'columns' => [ 'a', 'b' ], 'records' => [[1, 2], [3, 4]] })
		expect(MessagePackDb::Load).to receive(:load_table).exactly(2).times

    expect {
      MessagePackDb::Load.load io
    }.not_to raise_error
  end

  it 'correctly deserializes tables with multiple data objects' do
    io = StringIO.new "\xC7\x0F \x93\xA7mytable\x92\xA1a\xA1b\x02\x92\x92\x01\x02\x92\x03\x04\x92\x92\x01\x02\x92\x03\x04"

		expect(MessagePackDb::Load).to receive(:load_table).exactly(2).times.with('mytable', { 'columns' => [ 'a', 'b' ], 'records' => [[1, 2], [3, 4]] })

    expect {
      MessagePackDb::Load.load io
    }.not_to raise_error
  end

  it "should load custom Ruby serialized datetime objects" do
    io = StringIO.new "\xC7\x14 \x93\xA7mytable\x91\xA8datetime\x01\x91\x91\xC7\x19\x012014-01-01T12:20:00+00:00"

		expect(MessagePackDb::Load).to receive(:load_table).once.with('mytable', { 'columns' => [ 'datetime' ], 'records' => [[DateTime.new(2014, 1, 1, 12, 20)]] })

    expect {
      MessagePackDb::Load.load io
    }.not_to raise_error
  end

  it "should load custom Ruby serialized date objects" do
    io = StringIO.new "\xD8 \x93\xA7mytable\x91\xA4date\x01\x91\x91\xC7\n\x032014-01-01"

		expect(MessagePackDb::Load).to receive(:load_table).once.with('mytable', { 'columns' => [ 'date' ], 'records' => [[Date.new(2014, 1, 1)]] })

    expect {
      MessagePackDb::Load.load io
    }.not_to raise_error
  end

  it "should load custom Ruby serialized time objects" do
    io = StringIO.new "\xD8 \x93\xA7mytable\x91\xA4time\x01\x91\x91\xC7\x19\x022014-01-01 12:20:00 +0000"

		expect(MessagePackDb::Load).to receive(:load_table).once.with('mytable', { 'columns' => [ 'time' ], 'records' => [[Time.new(2014, 1, 1, 12, 20, 0)]] })

    expect {
      MessagePackDb::Load.load io
    }.not_to raise_error
  end

  it "should load custom Ruby serialized BigDecimal objects" do
    io = StringIO.new "\xC7\x16 \x93\xA7mytable\x91\xAAbigdecimal\x01\x91\x91\xC7\a\x041234.56"

		expect(MessagePackDb::Load).to receive(:load_table).once.with('mytable', { 'columns' => [ 'bigdecimal' ], 'records' => [[BigDecimal.new('1234.56')]] })

    expect {
      MessagePackDb::Load.load io
    }.not_to raise_error
  end
end
