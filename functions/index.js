const functions = require('firebase-functions');
const admin = require('firebase-admin');
admin.initializeApp(functions.config().firebase);
let db = admin.firestore();

const {google} = require("googleapis");

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


exports.read_sheets = functions.https.onRequest((request, response) => {

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
        sheets = await google.sheets({version: 'v4', auth});
    } catch (err) {
        response.status(500).write('Failed to get sheets')
    }

    console.log('Got sheets')
    return sheets
}


async function get_rows(sheets, sheet_id) {

    // TODO get the whole sheet without specifying the max index?
    const range = `Responses via online form!A2:Q${MAX_ROWS}`
    console.log(`Getting range ${range}`)
    const request = {
        spreadsheetId: sheet_id,
        range: range,
    }

    // https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get
    let data = (await sheets.spreadsheets.values.get(request)).data;

    let rows = await data.values.map((row) => make_row_obj(row))
    console.log(rows.length + ' rows')
    return rows
}

async function write_rows(rows) {

    // combine to one object
    var all_huge_doc = {} 
    rows.forEach((row) => all_huge_doc[sheettime_to_id(row.timestamp)] = row)  // add each org to public_huge_doc, keyed by org id constructed from timestamp
    let write_all_huge_doc = await db.collection('community_responses').doc('all').set(all_huge_doc);
    console.log('Wrote all data')

    // same again for public info only
    // yeah, I should refactor...
    var public_huge_doc = {} 
    rows.forEach((row) => {
        delete row.contact_first_name
        delete row.contact_last_name
        delete row.contact_email  // seperate to group email
        delete row.contact_telephone
        delete row.volunteer_count
        delete row.oai_help
        public_huge_doc[sheettime_to_id(row.timestamp)] = row
        return ;
    })
    let write_public_huge_doc = await db.collection('community_responses').doc('public').set(public_huge_doc);
    console.log('Wrote public data')

    return 'anything'  // need to return something for await in write_rows to work

    // outdated, one doc for all orgs now
    // const promises = rows.map(async row => {
    //     await write_row(row)
    // })
    // await Promise.all(promises)
    // return 'Writing complete'  // absolutely must return something, for .then() to work
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
    const schema = [
        'timestamp',
        'group_email',
        'group_name',
        'postcode',
        'locations',
        'link_primary',
        'link_social',
        'contact_first_name', // private
        'contact_last_name',  // private
        'contact_email',  // private
        'contact_telephone',  // private
        'group_type',
        'support_description',
        'volunteer_count',  // private
        'oai_help',  // private
        'group_description_extra',
        'group_purpose'
    ]
    var row = {}
    entries = schema.map((e, i) => [[e], row_arr[i]])  // i.e. pairs
    
    // add to row
    // return Object.fromEntries(entries);  // sadly not supported?
    entries.forEach((e, i) => row[e[0]] = e[1] )

    // remove any undefined properties
    Object.keys(row).forEach(key => {
        if (row[key] === undefined) {
          delete row[key];
        }
      });

    // add lat/long to object, calculated w/ API call
    postcodeToLatLong(row.postcode)
        .then((latlong) => {
            row.latitude = latlong[0]
            row.longitude = latlong[1]
            return 'complete'
            })
        .catch((x) => console.log('Caught'));  // catch must accept an arg
    return row
}

// modified from app script
async function postcodeToLatLong(raw_postcode) {
    const postcode = raw_postcode.toUpperCase()  // and the space?
    var url = 'https://api.postcodes.io/postcodes/' + encodeURIComponent(postcode);
    // requires node-fetch
// https://www.valentinog.com/blog/http-js/
    let response = await fetch(url, {'muteHttpExceptions': true});
    // console.log(response);
    let data = await response.json();
//   console.log(data)
    if (data.status === 200.0) {
        return [data.result.latitude, data.result.longitude];
    }
    console.log('Failed to geocode postcode '+ raw_postcode)
    throw new Error("Whoops!");
    // return 0;  // which causes latlong[1] to fail and therefore catch to trigger
}


function sheettime_to_id(s) {
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
}

exports.testing = functions.https.onRequest((request, response) => {


    db.collection("community_responses").listDocuments().then((docs) => response.send('Got ' + docs.length +' ...' + docs)).catch('Failed')

    // var s = "19/03/2020 10:44:29"

    // // Calling Date without the new keyword returns a string representing the current date and time.
    // // var d = Date.parse(s)
    // const i = sheettime_to_id(s)
    // response.status(200).write('id ' + i)
})



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



// // Create and Deploy Your First Cloud Functions
// // https://firebase.google.com/docs/functions/write-firebase-functions
exports.helloWorld = functions.https.onRequest((request, response) => {
    response.send("Hello from Firebase!");
   });



