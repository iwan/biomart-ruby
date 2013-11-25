require 'active_record'
require 'mysql2'
require 'nokogiri'
require 'open-uri'
require 'activerecord-import' # https://github.com/zdennis/activerecord-import/wiki
require 'uri'

@connection_params = {
  :adapter  => 'mysql2',
  :host     => 'localhost',
  :database => 'pippo',
  :username => 'root',
  :password => ''
}


gm_url = "http://genemania.org/data/current/networks/"
@tables = {} # :db_name => [ table_names ]
@log = []


doc = Nokogiri::HTML(open(gm_url))

class SkipError < StandardError
  def initialize(msg=nil)
    super
  end
end

class Table
  attr_accessor :name, :file_url, :file_size
  def initialize(name, file_url, file_size)
    @name = name
    @file_url = file_url
    @file_size = file_size
  end
end


def keep?(name, href, size_threshold=0.0)
  # name appear truncated when too long
  raise SkipError.new("is not a data file") if name[0]=="?" || name[0]=="/"
  slice = href.split(".").map(&:downcase)
  raise SkipError.new("is not a 'txt' file") if slice[-1]!="txt"
  ["predicted", "co-expression"].each do |s|
    raise SkipError.new("name start with '#{s}'") if slice[0]==s
  end
  raise SkipError.new("is not a desired file") if name=="identifier_mappings.txt"
  raise SkipError.new("is not a desired file") if name=="networks.txt"

  db_name = slice[0].gsub("-", "_")
  table_name = slice[1..-2].join("_").gsub("-", "_")
  return [db_name, table_name]

  # puts slice.inspect
end


def get_size(url)
  uri = URI(url)
  begin
    Net::HTTP.start(uri.host) do |http|
      head = http.head(uri.path)
      return head['content-length'] # in bytes
    end
  rescue Exception => e
    puts e.message
    0
  end
end

def load_subpage(base_url, specie)
  doc = Nokogiri::HTML(open(base_url+specie))
  doc.css("a").each_with_index do |el, i|
    href = el["href"]
    name = el.content
    begin
      db_name, table_name = keep?(name, href)
      db_name = "#{specie.downcase[0...-1]}_#{db_name}"
      @log << "Importing '#{name}' (db: #{db_name}, table: #{table_name})"
      txt_url = base_url+specie+href
      size = get_size(txt_url)
      limit = 21_000_000
      if size.to_i<limit # TODO: refactor code
        (@tables[db_name]||=[]) << Table.new(table_name, txt_url, size)      
      else
        raise SkipError.new("file too big (#{size.to_i/2**20}MB limit is #{limit/2**20})")
      end
    rescue Exception => e
      @log <<  "'#{name}' skipped because #{e.message}"
    end
  end
end

doc.css("a").each_with_index do |el, i|
  href = el["href"]
  # puts href
  if href[0]!="?" && href[0]!="/" && href[-1]=="/"
    puts "--------> #{href}"
    load_subpage(gm_url, href)
  end
end

def with_database(db_name=nil)
  params = @connection_params.dup
  params[:database] = db_name if db_name
  params
end


class CreateMyTable < ActiveRecord::Migration
  def self.up(table_name)
    create_table(table_name.to_sym) do |t|
      t.string :gene_a, limit: 32
      t.string :gene_b, limit: 32
      t.float :weight
    end
  end

  def self.down(table_name)
    drop_table table_name.to_sym
  end
end

@log << "----------------------------------------"
@log << "db count: #{@tables.size}"
@log << "table count: #{@tables.values.flatten.size}"

File.open("genemania.log", 'w') {|f| f.write(@log.join("\n")) }

File.open("db_struc.yml", 'w') do |f|
  tables = {}
  @tables.each do |k, v|
    tables[k] = v.collect(&:name)
  end
  f.write(tables.to_yaml)
end

File.open("db_struc.tsv", 'w') do |f|
  tables = []
  @tables.each do |k, v|
    v.each do |t|
      tables << [k, t.name, t.file_url, t.file_size].join("\t")      
    end
  end
  f.write(tables.join("\n"))
end

File.open("genemania_dropdb.sql", 'w') do |f|
  f.write(@tables.keys.collect{|db| "DROP DATABASE IF EXISTS #{db};"}.join("\n"))
end

# exit
# puts @tables.inspect

puts "\n\n\n\n----------------------------------------------------"

def recreate_db(db_name)
  ActiveRecord::Base.establish_connection(with_database)
  ActiveRecord::Base.connection.drop_database db_name rescue nil
  ActiveRecord::Base.connection.create_database db_name rescue nil
  puts "creating database #{db_name}"
end

import_every = 1000

@tables.each do |db_name, tables|
  recreate_db(db_name)
  tables.each do |table|
    ActiveRecord::Base.establish_connection(with_database(db_name))
    CreateMyTable.down(table.name) rescue nil
    puts "creating table '#{table.name}'"
    CreateMyTable.up(table.name)
    i=0
    records = []
    ar = Class.new(ActiveRecord::Base) do
      self.table_name = table.name
    end
    puts "opening: #{table.file_url}"
    open(table.file_url).each_line do |l|
      i+=1
      next if i==1
      l.chomp!
      z = l.split("\t")
      # ar.create(gene_a: z[0], gene_b: z[1], weight: z[2])
      records << ar.new(gene_a: z[0], gene_b: z[1], weight: z[2])

      if i%import_every==0
        puts "line: #{i}"
        ar.import(records)
        records = []
      end
    end
    ar.import(records) unless records.empty?
    # exit
  end
end
      
