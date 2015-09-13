#!/bin/env ruby
# encoding: utf-8

require 'scraperwiki'
require 'nokogiri'
require 'open-uri'
require 'colorize'
require 'rest-client'
require 'csv'

require 'pry'
require 'open-uri/cached'
OpenURI::Cache.cache_path = '.cache'

@API_URL = 'http://api.parldata.eu/sk/nrsr/%s'

def noko_q(endpoint, h)
  result = RestClient.get (@API_URL % endpoint), params: h
  warn result.request.url
  doc = Nokogiri::XML(result)
  doc.remove_namespaces!
  entries = doc.xpath('resource/resource')
  return entries if (np = doc.xpath('.//link[@rel="next"]/@href')).empty?
  return [entries, noko_q(endpoint, h.merge(page: np.text[/page=(\d+)/, 1]))].flatten
end

def earliest_date(*dates)
  dates.compact.reject(&:empty?).sort.first
end

def latest_date(*dates)
  dates.compact.reject(&:empty?).sort.last
end 

def overlap_old(gmem, cmem, term)
  mS = [gmem[:start_date], gmem[:faction_start], '0000-00-00'].find { |d| !d.to_s.empty? }
  mE = [gmem[:end_date],   gmem[:faction_end],   '9999-99-99'].find { |d| !d.to_s.empty? }
  tS = [cmem[:start_date], term[:start_date], '0000-00-00'].find { |d| !d.to_s.empty? }
  tE = [cmem[:end_date],   term[:end_date],   '9999-99-99'].find { |d| !d.to_s.empty? }

  return unless mS < tE && mE > tS
  (s, e) = [mS, mE, tS, tE].sort[1,2]
  return { 
    start_date: s == '0000-00-00' ? nil : s,
    end_date:   e == '9999-99-99' ? nil : e,
  }
end

def overlap(mem, term)
  mS = mem[:start_date].to_s.empty?  ? '0000-00-00' : mem[:start_date]
  mE = mem[:end_date].to_s.empty?    ? '9999-99-99' : mem[:end_date]
  tS = term[:start_date].to_s.empty? ? '0000-00-00' : term[:start_date]
  tE = term[:end_date].to_s.empty?   ? '9999-99-99' : term[:end_date]

  return unless mS < tE && mE > tS
  (s, e) = [mS, mE, tS, tE].sort[1,2]
  return { 
    _data: [mem, term],
    start_date: s == '0000-00-00' ? nil : s,
    end_date:   e == '9999-99-99' ? nil : e,
  }
end

def combine(h)
  into_name, into_data, from_name, from_data = h.flatten
  from_data.product(into_data).map { |a,b| overlap(a,b) }.compact.map { |h|
    data = h.delete :_data
    h.merge({ from_name => data.first[:id], into_name => data.last[:id] })
  }.sort_by { |h| h[:start_date] }
end


# http://api.parldata.eu/sk/nrsr/organizations?where={"classification":"chamber"}
terms = noko_q('organizations', where: %Q[{"classification":"chamber"}] )
terms.each do |chamber|
  term = { 
    id: chamber.xpath('.//identifiers[scheme[text()="nrsr.sk"]]/identifier').text,
    identifier__parldata: chamber.xpath('.//id').text,
    name: chamber.xpath('.//name').text,
    start_date: chamber.xpath('.//founding_date').text,
    end_date: chamber.xpath('.//dissolution_date').text,
  }
  ScraperWiki.save_sqlite([:id], term, 'terms')
end

# http://api.parldata.eu/sk/nrsr/organizations?where={"classification":"parliamentary group"}
groups = noko_q('organizations', where: %Q[{"classification":"parliamentary group"}] ).map do |group|
  data = { 
    id: group.xpath('.//identifiers[scheme[text()="nrsr.sk"]]/identifier').text,
    identifier__parldata: group.xpath('.//id').text,
    name: group.xpath('.//name').text,
    start_date: group.xpath('.//founding_date').text,
    end_date: group.xpath('.//dissolution_date').text,
  }
  ScraperWiki.save_sqlite([:id], data, 'factions')
  data
end

# http://api.parldata.eu/sk/nrsr/people?embed=[%22memberships.organization%22]
people = noko_q('people', { 
  max_results: 50,
  embed: '["memberships.organization"]' ,
})

people.each do |person|
  person.xpath('changes').each { |m| m.remove } # make eyeballing easier
  nrsr_id = person.xpath('.//identifiers[scheme[text()="nrsr.sk"]]/identifier').text
  person_data = { 
    id: nrsr_id,
    identifier__nrsr: nrsr_id,
    identifier__parldata: person.xpath('id').text,
    name: person.xpath('name').text,
    sort_name: person.xpath('sort_name').text,
    family_name: person.xpath('family_name').text,
    given_name: person.xpath('given_name').text,
    honorific_prefix: person.xpath('honorific_prefix').text,
    birth_date: person.xpath('birth_date').text,
    death_date: person.xpath('death_date').text,
    gender: person.xpath('gender').text,
    national_identity: person.xpath('national_identity').text,
    email: person.xpath('email').text,
    image: person.xpath('image').text,
    source: person.xpath('./sources[note[text()="Profil na webe NRSR"]]/url').text,
  }

  group_mems = person.xpath('memberships[organization[classification[text()="parliamentary group"]]]').map { |gm|
    {
      name: gm.xpath('organization/name').text,
      id: gm.xpath('.//identifiers[scheme[text()="nrsr.sk"]]/identifier').text,
      start_date: latest_date(gm.xpath('organization/founding_date').text, gm.xpath('start_date').text),
      end_date: earliest_date(gm.xpath('organization/dissolution_date').text, gm.xpath('end_date').text),
    }
  }

  term_mems = person.xpath('memberships[organization[classification[text()="chamber"]]]').map { |gm|
    {
      name: gm.xpath('organization/name').text,
      id: gm.xpath('.//identifiers[scheme[text()="nrsr.sk"]]/identifier').text,
      start_date: gm.xpath('start_date').text,
      end_date: gm.xpath('end_date').text,
    }
  }.reject { |tm| tm[:id].to_i == 1 } # no faction information available for term 1

  combine(term: term_mems, faction_id: group_mems).each do |mem|
    data = person_data.merge(mem)
    data[:faction] = groups.find { |g| g[:id] == mem[:faction_id] }[:name]
    puts data.to_s.cyan
    ScraperWiki.save_sqlite([:id, :term, :faction_id, :start_date], data.reject { |k,v| v.to_s.empty? })
  end

end
