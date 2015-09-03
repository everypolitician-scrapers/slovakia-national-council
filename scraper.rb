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

def overlap(mem, term)
  mS = mem[:start_date].to_s.empty?  ? '0000-00-00' : mem[:start_date]
  mE = mem[:end_date].to_s.empty?    ? '9999-99-99' : mem[:end_date]
  tS = term[:start_date].to_s.empty? ? '0000-00-00' : term[:start_date]
  tE = term[:end_date].to_s.empty?   ? '9999-99-99' : term[:end_date]

  return unless mS < tE && mE > tS
  (s, e) = [mS, mE, tS, tE].sort[1,2]
  return { 
    start_date: s == '0000-00-00' ? nil : s,
    end_date:   e == '9999-99-99' ? nil : e,
  }
end

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
  # warn term
  ScraperWiki.save_sqlite([:id], term, 'terms')

  # http://api.parldata.eu/sk/nrsr/memberships?where={"organization_id":"54d2a42b273a394ad5db921e"}&embed=["person.memberships.organization"]
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

    mems = person.xpath('memberships[organization[classification[text()="parliamentary group"]]]').map { |m|
      {
        party: m.xpath('organization/name').text,
        party_id: m.xpath('.//identifiers[scheme[text()="nrsr.sk"]]/identifier').text,
        start_date: m.xpath('start_date').text,
        end_date: m.xpath('end_date').text,
      }
    }.select { |m| overlap(m, term) } 

    if mems.count.zero?
      row = data.merge({
        party: 'Independent', 
        party_id: 'IND',
      })
      # puts row.to_s.red
      ScraperWiki.save_sqlite([:id, :term], row)
    else
      mems.each do |mem|
        range = overlap(mem, term) or raise "No overlap"
        row = data.merge(mem).merge(range)
        # puts row.to_s.magenta
        ScraperWiki.save_sqlite([:id, :term, :start_date], row)
      end
    end
  end
end

