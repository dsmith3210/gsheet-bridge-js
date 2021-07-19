const crypto = require('crypto');
const { google } = require('googleapis');
const sheets = google.sheets('v4');

const auth = new google.auth.GoogleAuth({
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
});

function convertObjectToRow(fields, newProps) {
    return fields.map(fieldName => {
        return newProps[fieldName] != null ? newProps[fieldName].toString().trim() : '';
    });
}

function zeroBasedColumnNumberToSpreadsheetColumnLetters(num) {
    const modulo = num % 26;
    const letter = String.fromCharCode("A".charCodeAt(0) + modulo);

    const remainder = Math.floor(num / 26);
    if (remainder === 0) {
        return letter;
    }

    return zeroBasedColumnNumberToSpreadsheetColumnLetters(remainder - 1) + letter;
}

function areEqual(a, b) {
    if (a === b) return true;
    if (!isNaN(Number(a)) && !isNaN(Number(b)) && Number(a) === Number(b)) return true;
    return false;
}

function dataMatchesQuery(dataRow, query) {
    if (!query) return true;
    for (const prop in query) {
        if (!query.hasOwnProperty(prop)) continue;
        if (!areEqual(dataRow[prop], query[prop])) return false;
    }

    return true;
}

module.exports = function(spreadsheetId, sheetName) {

    async function _query(query) {
        const { data } = await sheets.spreadsheets.values.get({
            spreadsheetId,
            auth,
            range: sheetName
        });

        const [fields, ...rest] = data.values; // get column names from first row
        fields[0] = 'ID';
        return rest.reduce((accum, row) => {
            const newItem = {};
            fields.forEach((item, index) => {
                const name = fields[index];
                newItem[name] = row[index]; // apply column name
            });
            if (dataMatchesQuery(newItem, query)) {
                accum.push(newItem);
            }
            return accum;
        }, []);
    }

    async function _fields() {
        const { data } = await sheets.spreadsheets.values.get({
            spreadsheetId,
            auth,
            range: sheetName
        });

        const [fields] = data.values; // get column names from first row
        fields[0] = 'ID';
        return fields;
    }

    async function _insert(newData) {
        const data = await _query();
        const fields = await _fields();

        if (!Array.isArray(newData)) newData = [newData];

        const newRows = [];

        for (const newProps of newData) {
            if (newProps.ID === undefined) {
                // auto generate an ID
                let proposedId;
                do {
                    proposedId = crypto.randomBytes(4).toString("hex").toUpperCase();
                } while (data.find(x => x.ID === proposedId) != null);
                newProps.ID = proposedId;
            }
            data.push(newProps);
            newRows.push(convertObjectToRow(fields, newProps));
        }

        await sheets.spreadsheets.values.append({
            auth,
            spreadsheetId,
            range: sheetName,
            valueInputOption: 'RAW',
            insertDataOption: 'INSERT_ROWS',
            resource: {
                values: newRows,
            },
        });

        return newData;
    }

    async function _update(query, values) {
        const data = await _query();

        if (data.length === 0) {
            console.log('0 rows returned for ', query);
            return;
        }

        const fields = await _fields();
        const updateData = [];
        const changedItems = [];

        // go over existing rows
        for (const dataEntry of Object.entries(data)) {
            const dataRow = dataEntry[1];
            if (!dataMatchesQuery(dataRow, query)) {
                continue;
            }
            const dataRowIndex = Number(dataEntry[0]);
            // go over all properties in the update
            let updatedSomething = false;
            let updatedDataRow = {...dataRow};
            for (const prop in values) {
                if (!values.hasOwnProperty(prop)) continue;
                const propValue = values[prop];
                updatedDataRow[prop] = propValue;
                // translate property key to sheet column number
                const columnNumber = fields.indexOf(prop);
                if (columnNumber === -1) {
                    throw new Error(`Bad field: ${prop}`);
                }
                const columnLetters = zeroBasedColumnNumberToSpreadsheetColumnLetters(columnNumber);
                const range = `${sheetName}!${columnLetters}${dataRowIndex + 2}`; // +1 because JS data is zero-based, sheet is 1-based. +1 because 1 row is header row.
                updatedSomething = true;
                updateData.push({
                    range,
                    values: [[propValue]]
                });
            }
            if (updatedSomething) {
                changedItems.push(updatedDataRow);
            }
        }

        await sheets.spreadsheets.values.batchUpdate({
            auth,
            spreadsheetId,
            resource: {
                valueInputOption: "RAW",
                data: updateData
            }
        });

        return changedItems;
    }

    return {
        query: _query,
        insert: _insert,
        update: _update,
        fields: _fields,
    };
};
