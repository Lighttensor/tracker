async function fetchData() {
    try {
        const response = await fetch('/api/data');
        const data = await response.json();
        updateTable(data);
    } catch (error) {
        console.error('Error fetching data:', error);
    }
}

function updateTable(data) {
    const container = document.getElementById('data-table');
    
    // Группировка по парам
    const groupedData = {};
    data.forEach(row => {
        if (!groupedData[row.market]) {
            groupedData[row.market] = [];
        }
        groupedData[row.market].push(row);
    });
    
    // Создание таблицы для каждой пары
    container.innerHTML = '';
    for (const [market, rows] of Object.entries(groupedData)) {
        const table = createMarketTable(market, rows);
        container.appendChild(table);
    }
}

function createMarketTable(market, rows) {
    const div = document.createElement('div');
    div.className = 'market-table';
    
    const h2 = document.createElement('h2');
    h2.textContent = market;
    div.appendChild(h2);
    
    const table = document.createElement('table');
    table.innerHTML = `
        <thead>
            <tr>
                <th>Time</th>
                <th>Open</th>
                <th>High</th>
                <th>Low</th>
                <th>Close</th>
                <th>Volume</th>
                <th>Source</th>
            </tr>
        </thead>
        <tbody>
            ${rows.map(row => `
                <tr>
                    <td>${row.candle_date_time_utc}</td>
                    <td>${row.opening_price}</td>
                    <td>${row.high_price}</td>
                    <td>${row.low_price}</td>
                    <td>${row.trade_price}</td>
                    <td>${row.candle_acc_trade_volume}</td>
                    <td>${row.source}</td>
                </tr>
            `).join('')}
        </tbody>
    `;
    
    div.appendChild(table);
    return div;
}

// Обновление данных каждые 5 секунд
setInterval(fetchData, 5000);
// Начальная загрузка
fetchData();
