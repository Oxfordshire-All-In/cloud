const functions = require('firebase-functions');
const admin = require('firebase-admin');
admin.initializeApp(functions.config().firebase);
let db = admin.firestore();

const {google} = require("googleapis");


exports.read_sheets = functions.https.onRequest((request, response) => {

    // const sheet_id = '1uo2ZwxV4cyigS8uFAUKa_n9zCMK_jy5z27IiKWh4q58';  // test
    const sheet_id = '11Gwlq47Et6sNK-Cfopli5XP_arI40NatrU4IvjQfxGM';  // real
    sheets_p = authenticate(response)

    sheets_p.then(
            (sheets_api) => get_rows(sheets_api, sheet_id)
        ) // success getting data
        // .then((value) => console.log('value ', value))  // log data
        // .then(response.send('Success'))
        // .then((value) => response.send(value))
        .then((rows) => write_rows(rows))
        .then(response.send("Write complete"))
        .catch(response.send('Failed'))
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
        response.send('Failed to get sheets')
    }

    console.log('Got sheets ' + sheets)
    return sheets
}

function make_row_obj(row_arr) {
    const schema = [
        'timestamp',
        'group_email',
        'group_name',
        'postcode',
        'locations',
        'link_primary',
        'link_social',
        'contact_first_name',
        'contact_last_name',
        'contact_email',
        'contact_telephone',
        'group_type',
        'support_description',
        'volunteer_count',
        'oai_help',
        'group_description_extra',
        'group_purpose'
    ]
    var row = {}
    entries = schema.map((e, i) => [[e], row_arr[i]])  // i.e. pairs
    // console.log('entries', entries)
    
    // add to row
    // return Object.fromEntries(entries);  // sadly not supported?
    entries.forEach((e, i) => row[e[0]] = e[1] )

    // remove any undefined properties
    Object.keys(row).forEach(key => {
        if (row[key] === undefined) {
          delete row[key];
        }
      });
    // console.log('row', row)
    return row
}


async function get_rows(sheets, sheet_id) {
    // console.log('Opening' + sheet_id + 'with' + sheets)

    const request = {
        spreadsheetId: sheet_id,
        range: 'Responses via online form!A2:Q50',
    }

    let data;
    data = (await sheets.spreadsheets.values.get(request)).data;
    console.log('Read data ', data)

    let rows;
    rows = await data.values.map((row) => make_row_obj(row))
    console.log('Converted to rows ', rows)
    return rows

    // let row;
    // row = await data.values[12]
    // console.log('Read row ', row)

    // // let value;
    // // value = await row[1]
    // // console.log('Read value ', value)

    // return make_row_obj(row)
}


// // Create and Deploy Your First Cloud Functions
// // https://firebase.google.com/docs/functions/write-firebase-functions
//
exports.helloWorld = functions.https.onRequest((request, response) => {
 response.send("Hello from Firebase!");
});

async function write_rows(rows) {

    // await rows.forEach((row, i) => write_row(row))
    // for (row in rows) {
    const promises = rows.map(async row => {
        await write_row(row)
        // .then(console.log('Confirming write successful'))
        // .catch(console.log)
    })
    await Promise.all(promises)
}

function write_row(row) {
    // console.log('Writing', row)
    let set_row = db.collection('community_responses').add(row)  // promise to write the row
        .then(console.log('Successfully wrote ', row))
        .catch(x => console.log('Failure writing: ', x));
    return set_row  // need to return the promise
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
