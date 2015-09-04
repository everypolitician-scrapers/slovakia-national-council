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
  doc = Nokogiri::XML(result)
  doc.remove_namespaces!
  entries = doc.xpath('resource/resource')
  return entries if (np = doc.xpath('.//link[@rel="next"]/@href')).empty?
  return [entries, noko_q(endpoint, h.merge(page: np.text[/page=(\d+)/, 1]))].flatten
end

def overlap(gmem, cmem, term)
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

@mem_id = 0

# http://api.parldata.eu/sk/nrsr/organizations?where={"classification":"chamber"}
xml = noko_q('organizations', where: %Q[{"classification":"chamber"}] )
xml.each do |chamber|
  term = { 
    id: chamber.xpath('.//identifiers[scheme[text()="nrsr.sk"]]/identifier').text,
    identifier__parldata: chamber.xpath('.//id').text,
    name: chamber.xpath('.//name').text,
    start_date: chamber.xpath('.//founding_date').text,
    end_date: chamber.xpath('.//dissolution_date').text,
  }
  puts term
  ScraperWiki.save_sqlite([:id], term, 'terms')

  # http://api.parldata.eu/sk/nrsr/memberships?where={"organization_id":"54d2a70d273a394ad5dbb870"}&embed=["person.memberships.organization"]
  mems = noko_q('memberships', { 
    where: %Q[{"organization_id":"#{term[:identifier__parldata]}"}],
    max_results: 50,
    embed: '["person.memberships.organization"]'
  })

  mems.each do |mem|
    person = mem.xpath('person')
    person.xpath('changes').each { |m| m.remove } # make eyeballing easier
    nrsr_id = person.xpath('.//identifiers[scheme[text()="nrsr.sk"]]/identifier').text
    data = { 
      id: nrsr_id,
      identifier__psp: nrsr_id,
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
      term: term[:id],
      source: person.xpath('./sources[note[text()="Profil na webe NRSR"]]/url').text,
    }
    # data.delete :sort_name if data[:sort_name] == ','

    cmem = {
      start_date: mem.xpath('start_date').text,
      end_date: mem.xpath('end_date').text,
    }

    gmems = person.xpath('memberships[organization[classification[text()="parliamentary group"]]]').map { |gm|
      {
        faction: gm.xpath('organization/name').text,
        faction_id: gm.xpath('.//identifiers[scheme[text()="nrsr.sk"]]/identifier').text,
        faction_start: gm.xpath('organization/founding_date').text,
        faction_end: gm.xpath('organization/dissolution_date').text,
        start_date: gm.xpath('start_date').text,
        end_date: gm.xpath('end_date').text,
      }
    }.select { |gm| overlap(gm, cmem, term) } 

    if gmems.count.zero?
      row = data.merge({
        faction: 'Independent', 
        faction_id: 'IND',
        mem_id: (@mem_id += 1),
      })
      # puts row.to_s.red
      ScraperWiki.save_sqlite([:mem_id], row)
    else
      gmems.each do |gmem|
        range = overlap(gmem, cmem, term) or raise "No overlap"
        row = data.merge(gmem).merge(range).merge({ mem_id: (@mem_id += 1) })
        # puts row.to_s.magenta if gmems.count > 1
        ScraperWiki.save_sqlite([:mem_id], row)
      end
    end
  end
end

