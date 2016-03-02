require 'json'

class Config
  # TODO: it
end

class LogParser

  def initialize
    @regexp_match = /([0-9\-]+[\s0-9:\.]+[\+0-9]{4,}) \[(.*)\] (\S+)([\s0-9\.s\(\)]+)? (.*)?/
    # config stuff eventually
  end

  def message_type(match_line)
    match_line[2].split("|")[0] unless match_line[2].nil?
  end

  def thread_id(match_line)
    match_line[2].split("|")[1] unless match_line[2].nil?
  end

  def message_source(match_line)
    match_line[2].split("|")[2] if match_line[2]
  end

  def execution_time(match_line)
    match_line[4][/[0-9\.]+/, 0] unless match_line[4].nil?
  end

  def parent_thread_id(match_line)
    match_line[5][/\|([0-9a-z]{5})\|/, 1] unless match_line[5].nil?
  end

  def action_type(match_line)
    match_line[5][/SELECT|UPDATE|DELETE|INSERT/, 0]
  end

  def which_table(match_line)
    if action_type(match_line) == 'UPDATE'
      match_line[5][/\"([A-Z_]+)\"/, 1]
    elsif action_type(match_line) == 'INSERT'
      match_line[5][/INTO \"([A-Z_]+)\"/, 1]
    else
      match_line[5][/FROM \"([A-Z_]+)\"/, 1]
    end
  end

  def java_error(match_line)
    match_line[5][/Java::[\w]+::[\w]+:[\w\s:\.]+/] if match_line[5]
  end

  def create_object(match_line)
    {
      :created_at => match_line[1] ? match_line[1] : "", 
      :message_type => message_type(match_line) ? message_type(match_line) : "",
      :message_source => message_source(match_line) ? message_source(match_line) : "",
      :execution_time => execution_time(match_line) ? execution_time(match_line) : "",
      :thread_id => thread_id(match_line) ? thread_id(match_line) : "",
      :parent_thread_id => parent_thread_id(match_line) ? parent_thread_id(match_line) : "",
      :action => action_type(match_line) ? action_type(match_line) : "",
      :query_table => which_table(match_line) ? which_table(match_line) : "",
      :java_error => java_error(match_line) ? java_error(match_line) : "",
      :full_message => match_line[5] ? match_line[5] : ""
    }
  end

  def run(log_line)
    log_line.match(@regexp_match)
  end

end

class Runner

  def initialize
    # file loader, eventually
    @log_parser = LogParser.new
  end

  # runner routine
  def run
    i = 0
    File.open("looker.log", "rb") do |input|    # TODO: non hardcoded
      File.open("looker.json", "a") do |output| # TODO: non hardcoded
        input.each_line do |line|
          if @log_parser.run(line)
            i += 1
            output.write("\{ \"index\" : \{ \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : #{i} \} \}" + "\n" + @log_parser.create_object(@log_parser.run(line)).to_json + "\n")
          else
            nil
          end
        end
      end
    end
  end

end
