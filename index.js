var r = require('rethinkdb');
var _ = require('lodash');
var fs = require('fs'), wstream = fs.createWriteStream('./new_test.xml');
var alphaSort = require('alpha-sort');
var request = require('request');
var elasticsearch = require('elasticsearch');

const INDEX = 'booodl-search';
const TYPE = 'stores';
const LOCAL = "52.63.90.234:9200";
var counter = 0;
var booodl_prod_client = new elasticsearch.Client({
  requestTimeout: 60000,
  host: LOCAL,
  cluster_name: 'elasticsearch',
  log: 'info'
});

function get_brands_for_city(city, state, callback) {
  booodl_prod_client.search({
    index: INDEX,
    type: TYPE,
    size: 150000,
    search_type: 'scan',
    scroll: '30s',
    q: "state:" + state
  }, function get_some(error, response) {
    do_checks(error, response, get_some);
    if(0 < response.hits.hits.length) {

      var bulk_body = [];
      hits = response.hits.hits;

      hits = _.filter(hits, function(store) {
        return store._source.city === city;
      });

      hits = _.map(hits, function(store) {

        store._source.brands = _.map(store._source.brands, function(brand) {
          return brand.name.replace(/ /g, "+");
        });
        return store._source.brands
      });

      hits = _.flatten(hits);
      hits = _.uniq(hits);
      callback(hits)
    }
  });
}

function do_checks(error, response, search_callback) {
  if(error){
    console.log('Error in do checks: ', error);
    return;
  }
  if(response._shards.total === 0){
    console.log("all done");
    return;
  }
  if(0 >= response.hits.hits.length){
    console.log("start");
    booodl_prod_client.scroll({
      scroll: '60s',
      scrollId: response._scroll_id,
      requestTimeout: 60000
    }, search_callback);
  } else {
    console.log("Another one...");
  }
}

function get_categories_for_city(city, state, callback) {
  console.log(state);
  r.connect(
    {},
    function(err, conn) {
      r.db('motoko').table('stores').filter({"state":state}).filter({"city":city}).group("topLevelCategory").count().run(conn, function(err, response) {
        response = _.map(response, function(element) {
          return element.group;
        });
        conn.close();
        callback(response);
      })
    });
}

function get_all_places(callback) {
  r.connect(
    {},
    function(err, conn) {
      r.db('motoko').table('places').pluck("slug", "state", "name").group("slug").run(conn, function(err, response) {
        response = _.map(response, function(element) {
          return {name: element.reduction[0].name, slug: element.reduction[0].slug, state: element.reduction[0].state};
        });
        conn.close();
        callback(response, conn);
      })
    });
}

function cycle_places(places_list) {
  if(places_list.length < 1) {
    return;
  } else {
    var head, tail, [head, ...tail] = places_list;
    // head.name
    // head.slug
    date = "2016-08-16";
    base_url = "https://booodl.com" + head.slug + "/category/";

    get_categories_for_city(head.name, head.state, function(categories) {
      lines = _.map(categories, function(cat){
        result = "<url><loc>" + base_url + cat + "</loc><lastmod>2016-08-16</lastmod></url>\n";
        console.log(result);
        return result;
      }).sort(alphaSort.asc).join("");
      wstream.write(lines);
      cycle_places(tail);
    })
  }
}

function cycle_places_2(places_list) {
  if(places_list.length < 1) {
    return;
  } else {
    var head, tail, [head, ...tail] = places_list;
    date = "2016-08-16";
    base_url = "https://booodl.com" + head.slug + "/brand/";

    console.log("\t\t\t" + head.state + " : " + head.name);
    get_brands_for_city(head.name, head.state, function(brands) {
      if(brands.length > 0) {
        lines = _.map(brands, function(brand){
          counter ++;

          result = "<url><loc>" + base_url + brand + "</loc><lastmod>2016-08-16</lastmod></url>\n";
          console.log(result);
          return result;
        }).sort(alphaSort.asc).join("");
        wstream.write(lines);
      } else {
        console.log("\t\t\tnope")
      }
      cycle_places_2(tail);
    })
  }
}

get_all_places(function(places) {
  cycle_places_2(places);
});
