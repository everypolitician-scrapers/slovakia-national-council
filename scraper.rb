#!/bin/env ruby
# encoding: utf-8

require 'scraperwiki'
require 'nokogiri'
require 'open-uri'
require 'rest-client'
require 'csv'

require 'pry'
require 'open-uri/cached'
OpenURI::Cache.cache_path = '.cache'

@API_URL = 'http://api.parldata.eu/sk/nrsr/%s'

def noko_q(endpoint, h)
  result = RestClient.get (@API_URL % endpoint), params: h, accept: :xml
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

# The source data has no Group memberships for periods where the person
# was independent. So we need to fill these in with a pseudo-party
def fill_independents(mems)
  ind = { name: "Independent", id: "0" }

  sorted = mems.map do |m|
    {
      id: m[:id],
      name: m[:name],
      start_date: m[:start_date].to_s.empty?  ? Date.parse('1001-01-01') : Date.parse(m[:start_date]),
      end_date: m[:end_date].to_s.empty?  ? Date.parse('9001-01-01') : Date.parse(m[:end_date]),
    }
  end.sort_by { |m| m[:start_date] }

  if sorted.size == 0
    sorted = [ ind ]
  else
    sorted.unshift ind.merge( end_date: sorted.first[:start_date] - 1 ) unless sorted.first[:start_date].to_s == '1001-01-01'
    sorted.push    ind.merge( start_date: sorted.last[:end_date] + 1  ) unless sorted.last[:end_date].to_s == '9001-01-01'
  end

  all = sorted.each_cons(2).map do |one, two|
    gap = ind.merge({
      start_date: one[:end_date] + 1,
      end_date: two[:start_date] - 1,
    })
    gap = nil if gap[:end_date] - gap[:start_date] < 2
    [one, gap, two]
  end.flatten.compact.uniq.map { |r|
    r.delete :start_date if r[:start_date].to_s < '1010-01-01' || r[:start_date].to_s > '9000-01-01'
    r.delete :end_date   if r[:end_date].to_s < '1010-01-01' || r[:end_date].to_s > '9000-01-01'
    r
  }
end

def overlap(mem, term)
  mS = mem[:start_date].to_s.empty?  ? '0000-00-00' : mem[:start_date].to_s
  mE = mem[:end_date].to_s.empty?    ? '9999-99-99' : mem[:end_date].to_s
  tS = term[:start_date].to_s.empty? ? '0000-00-00' : term[:start_date].to_s
  tE = term[:end_date].to_s.empty?   ? '9999-99-99' : term[:end_date].to_s

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

ScraperWiki.sqliteexecute('DROP TABLE data') rescue nil

# http://api.parldata.eu/sk/nrsr/organizations?where={"classification":"chamber"}
terms = noko_q('organizations', where: %Q[{"classification":"chamber"}] ).map do |chamber|
  {
    id: chamber.xpath('.//identifiers[scheme[text()="nrsr.sk"]]/identifier').text,
    identifier__parldata: chamber.xpath('.//id').text,
    name: chamber.xpath('.//name').text,
    start_date: chamber.xpath('.//founding_date').text,
    end_date: chamber.xpath('.//dissolution_date').text,
  }
end
ScraperWiki.save_sqlite([:id], terms, 'terms')

# http://api.parldata.eu/sk/nrsr/organizations?where={"classification":"parliamentary group"}
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

# people = noko_q('people', {
  # where: %Q({"family_name":"Bugár"}),
  # max_results: 50,
  # embed: '["memberships.organization"]' ,
# })

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

  term_mems = person.xpath('memberships[organization[classification[text()="chamber"]]]').map { |gm|
    id = gm.xpath('.//identifiers[scheme[text()="nrsr.sk"]]/identifier').text
    term = terms.find { |t| t[:id] == id }
    {
      id: id,
      name: gm.xpath('organization/name').text,
      start_date: latest_date(gm.xpath('start_date').text, term[:start_date]),
      end_date: earliest_date(gm.xpath('end_date').text, term[:end_date]),
    }
  }.reject { |tm| tm[:id].to_i == 1 } # no faction information available for term 1

  group_mems = fill_independents(person.xpath('memberships[organization[classification[text()="parliamentary group"]]]').map { |gm|
    {
      name: gm.xpath('organization/name').text,
      id: gm.xpath('.//identifiers[scheme[text()="nrsr.sk"]]/identifier').text,
      start_date: latest_date(gm.xpath('organization/founding_date').text, gm.xpath('start_date').text),
      end_date: earliest_date(gm.xpath('organization/dissolution_date').text, gm.xpath('end_date').text),
    }
  })

  combine(term: term_mems, faction_id: group_mems).each do |mem|
    data = person_data.merge(mem)
    data[:faction] = groups.find(->{{name: 'Independent'}}) { |g| g[:id] == mem[:faction_id] }[:name]
    ScraperWiki.save_sqlite([:id, :term, :faction_id, :start_date], data.reject { |k,v| v.to_s.empty? })
  end

end
