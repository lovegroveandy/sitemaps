var elasticsearch = require('elasticsearch');
var colors = require('colors');
var r = require('rethinkdb');
var _ = require('lodash');
var fs = require('fs'), wstream = fs.createWriteStream('./potato_test.xml');
var alphaSort = require('alpha-sort');

const INDEX = 'booodl-search';//eg booodl-search
const TYPE = 'stores';//eg stores, brands, categories
const ES_HOST = '52.63.90.234:9200';//eg ipaddress:port

var all_stores = [], vic_stores, act_stores, nsw_stores, sa_stores, wa_stores, qld_stores, tas_stores, nt_stores;
var counter = 0;
var booodl_prod_client = new elasticsearch.Client({
  requestTimeout: 60000,
  host: ES_HOST,
  cluster_name: 'elasticsearch',
  log: 'info'
});

booodl_prod_client.search({
  index: INDEX,
  type: TYPE,
  size: 1000,
  search_type: 'scan',
  scroll: '60s'
}, function get_some(error, response) {
  do_checks(error, response, get_some);
  if(0 < response.hits.hits.length) {

    var bulk_body = [];
    hits = response.hits.hits;
    console.log("here");

    hits.forEach(function(store) {
      all_stores.push(store._source);
    });

    booodl_prod_client.scroll({
      scroll: '60s',
      scrollId: response._scroll_id,
      requestTimeout: 60000
    }, get_some);
  }
});

function do_checks(error, response, search_callback) {
  if(error){
    console.log('Error in do checks: ', error);
    return;
  }
  if(response._shards.total === 0){
    console.log(all_stores.length);
    console.log("all done fetching");
    do_stuff();
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

function get_brands_for_city(city, state, callback) {
  var hits;
  switch(state) {
    case "NSW" :
    hits = nsw_stores;
    break;
    case "QLD" :
    hits = qld_stores;
    break;
    case "VIC" :
    hits = vic_stores;
    break;
    case "NT" :
    hits = nt_stores;
    break;
    case "SA" :
    hits = sa_stores;
    break;
    case "WA" :
    hits = wa_stores;
    break;
    case "ACT":
    hits = act_stores;
    break;
    case "TAS" :
    hits = tas_stores;
    break;
  }

  console.log("\tgetting brands for " + city);
  // hits = _.filter(_.cloneDeep(hits), function(store) {
  //   // console.log("\t\t\t\t\t\t\t\t" + store.name)
  //   return store.city === city;
  // });

  hits = _.map(_.cloneDeep(hits), function(store) {
    if(store.city !== city) {
      return null;
    } else {
      store.brands = _.map(store.brands, function(brand) {
        return brand.name.replace(/ /g, "+");
      });
      return store.brands;
    }
  });
  hit = _.filter(hits, function(store) {
    return store;
  });
  hits = _.flatten(hits);
  // hits = _.uniq(hits)

  console.log("\t\t got brands for " + city);
  callback(hits)
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

function cycle_places_2(places_list) {
  if(places_list.length < 1) {
    console.log("DONE BABY!");
    return;
  } else {
    var head, tail, [head, ...tail] = places_list;
    var date = "2016-08-28";
    base_url = "https://booodl.com" + head.slug + "/brand/";

    console.log(head.state + " : " + head.name);
    get_brands_for_city(head.name, head.state, function(brands) {
      console.log("\t\t\twriting " + brands.length + " entries");
      _.map(brands, function(brand){
        wstream.write("<url><loc>" + base_url + brand + "</loc><lastmod>" + date + "</lastmod></url>\n");
        return brand
      });

      cycle_places_2(tail);
    })
  }
}

function do_stuff() {

  act_stores = _.filter(_.cloneDeep(all_stores), function(store) {
    return store.state ==="ACT" && store.brands.length > 0;
  });
  console.log("done act " + act_stores.length);
  wa_stores = _.filter(_.cloneDeep(all_stores), function(store) {
    return store.state ==="WA" && store.brands.length > 0;
  });
  console.log("done wa " + wa_stores.length);
  nsw_stores = _.filter(_.cloneDeep(all_stores), function(store) {
    return store.state ==="NSW" && store.brands.length > 0;
  });
  console.log("done nsw " + nsw_stores.length);
  qld_stores = _.filter(_.cloneDeep(all_stores), function(store) {
    return store.state ==="QLD" && store.brands.length > 0;
  });
  console.log("done qld " + qld_stores.length);
  sa_stores = _.filter(_.cloneDeep(all_stores), function(store) {
    return store.state ==="SA" && store.brands.length > 0;
  });
  console.log("done sa " + sa_stores.length);
  tas_stores = _.filter(_.cloneDeep(all_stores), function(store) {
    return store.state ==="TAS" && store.brands.length > 0;
  });
  console.log("done tas " + tas_stores.length);
  vic_stores = _.filter(_.cloneDeep(all_stores), function(store) {
    return store.state ==="VIC" && store.brands.length > 0;
  });
  console.log("done vic " + vic_stores.length);
  nt_stores = _.filter(_.cloneDeep(all_stores), function(store) {
    return store.state ==="NT" && store.brands.length > 0;
  });
  console.log("done nt " + nt_stores.length);

  get_all_places(function(places) {
    cycle_places_2(places);
  });
}
