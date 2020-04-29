const functions = require('firebase-functions');
const admin = require('firebase-admin');
admin.initializeApp(functions.config().firebase);
let db = admin.firestore();

const {
  google
} = require("googleapis");

const fetch = require("node-fetch");

const MAX_ROWS = 800

// deploy to GCP with
// firebase deploy --only functions
// firebase deploy --only functions:read_sheets
// https://us-central1-mapping-7c4a8.cloudfunctions.net/read_sheets

// might be easier to debug locally, if firebase auth can be set up
// https://firebase.google.com/docs/firestore/quickstart?authuser=1
// https://support.google.com/firebase/answer/7015592?authuser=1

// useful node links
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/async_function


exports.read_sheets = functions.region("europe-west2").https.onRequest((request, response) => {

  const sheet_id = '11Gwlq47Et6sNK-Cfopli5XP_arI40NatrU4IvjQfxGM';
  sheets_p = authenticate(response)

  sheets_p
    .then((sheets_api) => get_rows(sheets_api, sheet_id))
    .then((rows) => write_rows(rows))
    .then((write_output) => response.send("Write: " + (write_output)))
    .catch((x) => response.status(500).send('Failed ' + x))
});


async function authenticate(response) {

  // block on auth + getting the sheets API object
  const auth = await google.auth.getClient({
    scopes: [
      "https://www.googleapis.com/auth/spreadsheets",
      "https://www.googleapis.com/auth/devstorage.read_only"
    ]
  });
  console.log("Auth complete");

  // block while the sheets promise completes
  let sheets;
  try {
    sheets = await google.sheets({
      version: 'v4',
      auth
    });
  } catch (err) {
    response.status(500).write('Failed to get sheets')
  }

  console.log('Got sheets')
  return sheets
}


async function get_rows(sheets, sheet_id) {

  // TODO get the whole sheet without specifying the max index?
  const range = `Responses via online form!A2:AL${MAX_ROWS}`
  console.log(`Getting range ${range}`)
  const request = {
    spreadsheetId: sheet_id,
    range: range,
  }

  // https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get
  let data = (await sheets.spreadsheets.values.get(request)).data;

  // https://flaviocopes.com/javascript-async-await-array-map/
  let rows = await Promise.all(data.values.map((row) => make_row_obj(row)))
  console.log('Read ' + rows.length + ' rows')
  console.log(rows[1]) // example row to log
  console.log(rows[46]) // example row to log
  console.log(rows[47]) // example row to log
  console.log(rows[50]) // example row to log
  console.log(rows[51]) // example row to log
  return rows
}

async function write_rows(rows) {

  console.log('Rows to write: ' + rows.length)
  console.log('example row to write: ', JSON.stringify(rows[0]))

  const public_fields = [
    'timestamp',
    'group_name',
    'postcode',
    'locations',
    'link_primary',
    'link_social',
    'contact_first_name', // now public
    'contact_last_name', // now public
    'group_email',
    'group_type',
    'support_description',
    'volunteer_count', // now public
    'group_description_extra',
    'group_purpose',
    'organisational_phone', // new
    'latitude', // not in header, added by geocoding
    'longitude', // similarly
    // underscore to denote dev fields
    // '_validEmail',
    // '_validUrl'
  ]
  console.log('Public fields: ' + public_fields)
  const private_fields = public_fields.concat([
    'contact_email',
    'contact_telephone'
  ])
  console.log('Private fields: ' + private_fields)

  // same again for public info only
  // yeah, I should refactor...
  console.log('public first')
  var public_huge_doc = {}

  ids = rows.map((row) => sheettime_to_id(row.timestamp))
  console.log('Made ' + ids.length + 'ids')
  var counts = {};
  for (var i = 0; i < ids.length; i++) {
    counts[ids[i]] = 1 + (counts[ids[i]] || 0);
  }
  console.log(counts)
  duplicate_times = Object.keys(counts).filter((id) => counts[id] > 1)
  if (duplicate_times.length > 0) {
    // these repeat - go check the sheet
    // http://extraconversion.com/base-number/base-36 then https://www.epochconverter.com/ for raw timestamp
    throw duplicate_times
  }

  rows.forEach((row) => public_huge_doc[sheettime_to_id(row.timestamp)] = select_fields(row, public_fields)) // add each org to public_huge_doc, keyed by org id constructed from timestamp
  console.log('Added ' + Object.keys(public_huge_doc).length + 'orgs to public doc object')
  // and log again
  example_org_id = sheettime_to_id(rows[1].timestamp)
  console.log('Example public org: ' + JSON.stringify(public_huge_doc[example_org_id]))
  public_doc_id = 'public_' + current_date_string()
  let write_public_huge_doc = await db.collection('orgs_public').doc(public_doc_id).set(public_huge_doc);
  console.log('Wrote public data to ' + public_doc_id)


  // combine to one object
  var private_huge_doc = {}
  rows.forEach((row) => private_huge_doc[sheettime_to_id(row.timestamp)] = select_fields(row, private_fields)) // add each org to public_huge_doc, keyed by org id constructed from timestamp
  // log an example private org map
  example_org_id = sheettime_to_id(rows[1].timestamp)
  console.log('Added ' + Object.keys(private_huge_doc).length + 'orgs to private doc object')
  console.log('Example private org: ' + JSON.stringify(private_huge_doc[example_org_id]))
  // write to db
  private_doc_id = 'private_' + current_date_string()
  let write_private_huge_doc = await db.collection('orgs_private').doc(private_doc_id).set(private_huge_doc);
  console.log('Wrote private data to ' + private_doc_id)


  return 'Successful write to ' + current_date_string() // need to return something for await in write_rows to work

  // outdated, one doc for all orgs now
  // const promises = rows.map(async row => {
  //     await write_row(row)
  // })
  // await Promise.all(promises)
  // return 'Writing complete'  // absolutely must return something, for .then() to work
}

// https://stackoverflow.com/questions/38750705/filter-object-properties-by-key-in-es6
function select_fields(raw, fields) {
  // get all keys
  return Object.keys(raw)
    // filter to keys you want
    .filter(key => fields.includes(key))
    // copy raw[key] to new object[key]
    .reduce((obj, key) => {
        obj[key] = raw[key];
        return obj;
    }, {});
}

// outdated, one doc for all orgs now
// async function write_row(row) {
//     // https://googleapis.dev/nodejs/firestore/latest/CollectionReference.html#add
//     // use timestamp as id
//     let doc_path = await sheettime_to_id(row.timestamp);
//     let set_row = await db.collection('community_responses').doc(doc_path).set(row);

//     console.log('Wrote ', doc_path)
//     return set_row  // need to return the promise for await in write_rows to work
// }

function make_row_obj(row_arr) {
  // this is the assumed header for google sheets.
  // it must exactly match the sheet columns
  // TODO add validation checks to make sure it does
  const schema = [
    'timestamp', // oai only
    'contact_email', // private
    'group_name',
    'postcode',
    'locations',
    'link_primary',
    'link_social',
    'contact_first_name', // now public
    'contact_last_name', // now public
    'organisational_phone', // public
    'group_email',
    'contact_telephone', // private
    'group_type',
    'support_description',
    'volunteer_count', // now public
    'oai_help', // oai only
    'group_description_extra',
    'group_purpose',
    'activities_or_services', // oai only
    'daily_bulletin', //oai only
    'other_useful_support', // oai only
    'business_support', // oai only
    'how_can_oai_help', // all oai only from here until org phone
    'summarise_covid_response',
    'how_can_oai_support',
    'set_up_call',
    'further_requests',
    'personal_data_accept',
    'contact_consent',
    'use_daily_bulletin',
    'other_support_needs',
    'how_else_can_oai_support',
    'organisational_needs',
    'identify_priority_needs',
    'activities_or_services_v2',
    'describe_beneficiaries',
    'oxford_council_confirmation',
    'personal_contact_email_redundant'
  ]
  var row = {}
  entries = schema.map((e, i) => [
    [e], row_arr[i]
  ]) // i.e. pairs

  // add to row
  // return Object.fromEntries(entries);  // sadly not supported?
  entries.forEach((e, i) => row[e[0]] = e[1])

  // remove any undefined properties
  Object.keys(row).forEach(key => {
    if (row[key] === undefined) {
      delete row[key];
    }
  });

  //  TODO add serverside checks
  // row._validURL = validURL(row.link_primary)
  // row._val

  // add lat/long to object, calculated w/ API call
  // .then and .catch will also return promise, so whole func returns promise of a row
  return postcodeToLatLong(row.postcode)
    .then((latlong) => {
      row.latitude = latlong[0]
      row.longitude = latlong[1]
      return row
    })
    .catch((x) => { // catch must accept an argument
      console.log('Caught')
      return row
    })
}

// modified from app script
async function postcodeToLatLong(raw_postcode) {
  const postcode = raw_postcode.toUpperCase() // and the space?
  var url = 'https://api.postcodes.io/postcodes/' + encodeURIComponent(postcode);
  // requires node-fetch
  // https://www.valentinog.com/blog/http-js/
  let response = await fetch(url, {
    'muteHttpExceptions': true
  });
  // console.log(response);
  let data = await response.json();
  //   console.log(data)
  if (data.status === 200.0) {
    return [data.result.latitude, data.result.longitude];
  }
  console.log('Failed to geocode postcode ' + raw_postcode)
  throw new Error("Whoops!");
  // return 0;  // which causes latlong[1] to fail and therefore catch to trigger
}

// generate unique hash ID for each org
function sheettime_to_id(s) {
  try {
    var parts = s.split(' ')
    date = parts[0]
    // console.log(date)
    day = date.slice(0, 2)
    month = date.slice(3, 5)
    year = date.slice(6, 10)
    time = parts[1]
    // console.log(time)
    hour = time.slice(0, 2)
    minute = time.slice(3, 5)
    second = time.slice(6, 8)
    // console.log(d)
    // console.log(d.toString(36))
    d = new Date(year, month, day, hour, minute, second)
    i = Number(d).toString(36)
    // console.log(i)
    return i
  } catch (err) {
    console.log(err)
    custom_str = 'Failed to get id from timestamp: ' + s + ' (undefined if not shown - missing from sheet?)'
    throw custom_str
  }

}

// generate date string for naming new document
function current_date_string() {
  const today = new Date();
  const dd = String(today.getDate()).padStart(2, '0');
  const mm = String(today.getMonth() + 1).padStart(2, '0'); //January is 0!
  const yyyy = today.getFullYear();
  return yyyy + '_' + mm + '_' + dd;
}

// Checks if URL is valid
function validURL(str) {
  var pattern = new RegExp('^(https?:\\/\\/)?(www\\.)?' + // protocol
    '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.)+[a-z]{2,}|' + // domain name
    '((\\d{1,3}\\.){3}\\d{1,3}))' + // OR ip (v4) address
    '(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*' + // port and path
    '(\\?[;&a-z\\d%_.~+=-]*)?' + // query string
    '(\\#[-a-z\\d_]*)?$', 'im'); // fragment locator
  return Boolean(pattern.test(str));
}

// Checks if email address is valid
function validEmail(str) {
  var pattern = new RegExp('^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,9}$', 'i');
  return Boolean(pattern.test(str))
}


exports.write_test = functions.https.onRequest((request, response) => {
  let docRef = db.collection('users').doc('alovelace');

  let setAda = docRef.set({
    first: 'Ada',
    last: 'Lovelace',
    born: 1815
  });

  let aTuringRef = db.collection('users').doc('aturing');

  let setAlan = aTuringRef.set({
    'first': 'Alan',
    'middle': 'Mathison',
    'last': 'Turing',
    'born': 1912
  });

  response.send("Write complete, may or may not be successful");

});
