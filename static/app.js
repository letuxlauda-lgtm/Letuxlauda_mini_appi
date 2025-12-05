// Инициализация Telegram Web App
let tg = window.Telegram.WebApp;
tg.expand();
tg.enableClosingConfirmation();

// Тема
document.documentElement.style.setProperty('--tg-theme-bg-color', tg.themeParams.bg_color || '#ffffff');
document.documentElement.style.setProperty('--tg-theme-text-color', tg.themeParams.text_color || '#000000');
document.documentElement.style.setProperty('--tg-theme-hint-color', tg.themeParams.hint_color || '#999999');
document.documentElement.style.setProperty('--tg-theme-button-color', tg.themeParams.button_color || '#5288c1');
document.documentElement.style.setProperty('--tg-theme-button-text-color', tg.themeParams.button_text_color || '#ffffff');
document.documentElement.style.setProperty('--tg-theme-secondary-bg-color', tg.themeParams.secondary_bg_color || '#f4f4f5');

// Пользователи
const users = {
    'sup1': { password: 'sup1$', role: 'super', name: 'Супервізор' },
    'rus1': { password: 'rus1$', role: 'tech', name: 'Руслан', tech: 'ruslan' },
    'callcentr1': { password: 'callcentr1$', role: 'callcenter', name: 'Call-центр' },
    'igor1': { password: 'igor1$', role: 'tech', name: 'Ігор', tech: 'igor' },
    'dmut1': { password: 'dmut1$', role: 'tech', name: 'Дмитро', tech: 'dmutro' },
    // ВАЖНО: Добавил tech: 'texdir', чтобы работали функции техника (расходы и просмотр задач)
    'texdir1': { password: 'texdir1$', role: 'texdir', name: 'Техдиректор', tech: 'texdir' } 
};

let currentUser = null;
let allAddresses = []; 
let selectedAparat = null; 
let currentProblem = null;
let selectedCardAparat = null;
let selectedUrgentAparat = null;
let selectedTerminAparat = null; // Для задач с термином

// Загрузка
document.addEventListener('DOMContentLoaded', async () => {
    try {
        const res = await fetch('/api/addresses');
        allAddresses = await res.json();
    } catch (e) { console.error(e); }

    const addressInput = document.getElementById('address-input');
    if (addressInput) {
        setupSearch(addressInput, 'search-results', (item) => selectAddress(item));
    }
});

// Универсальный поиск
function setupSearch(inputElement, resultsId, onSelectCallback) {
    const resultsBox = document.getElementById(resultsId);
    const newInput = inputElement.cloneNode(true);
    inputElement.parentNode.replaceChild(newInput, inputElement);

    newInput.addEventListener('input', function() {
        const query = this.value.toLowerCase();
        resultsBox.innerHTML = '';
        if (query.length < 2) { resultsBox.classList.remove('active'); return; }
        
        const filtered = allAddresses.filter(item => item.addr.toLowerCase().includes(query));
        if (filtered.length > 0) {
            resultsBox.classList.add('active');
            filtered.forEach(item => {
                const div = document.createElement('div');
                div.className = 'search-item';
                div.innerHTML = `<strong>[${item.id}]</strong> ${item.addr} <span style="font-size:10px; color:#888">${item.tech}</span>`;
                div.onclick = () => {
                    onSelectCallback(item);
                    newInput.value = item.addr;
                    resultsBox.classList.remove('active');
                };
                resultsBox.appendChild(div);
            });
        } else resultsBox.classList.remove('active');
    });
}

// Вход
function handleLogin() {
    const login = document.getElementById('login-input').value.trim();
    const password = document.getElementById('password-input').value;
    
    if (!users[login] || users[login].password !== password) {
        tg.showAlert('Невірний логін або пароль');
        return;
    }
    
    currentUser = { login, ...users[login] };
    document.getElementById('login-screen').classList.remove('active');
    
    switch (currentUser.role) {
        case 'tech': showTechCabinet(); break;
        case 'callcenter': showCallCenterCabinet(); break;
        case 'texdir': showTexdirCabinet(); break;
        case 'super': 
            showSuperCabinet(); 
            fetch('/api/super/export_mes_csv', { method: 'POST' }).catch(console.error);
            break;
    }
    sendToBot({ action: 'login', role: currentUser.role, name: currentUser.name });
}

// --- CALL CENTER ---
function selectAddress(item) {
    selectedAparat = item;
    document.getElementById('btn-send-task').disabled = false;
    document.getElementById('btn-send-task').innerHTML = `Відправити на <b>${item.tech}</b>`;
}

async function sendTaskToServer() {
    if (!selectedAparat || !currentProblem) return;
    const btn = document.getElementById('btn-send-task');
    btn.innerText = 'Відправка...';
    try {
        const response = await fetch('/api/create_task', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id: selectedAparat.id, problem: currentProblem })
        });
        const result = await response.json();
        if (result.status === 'success') { closeAddressModal(); tg.showAlert(result.message); } 
        else alert(result.message);
    } catch (e) { alert('Помилка'); } finally { btn.innerText = 'Відправити'; }
}

function handleCallCenterAction(action) {
    const data = { action: action, role: 'callcenter' };
    switch (action) {
        case 'new-task': showProblemMenu(); break;
        case 'new-card': openCardOrderModal(); break;
        case 'urgent': openUrgentModal(); break;
        case 'status': openStatusModal(); break;
    }
}

// --- ТЕХНИК ---
async function handleTechAction(action) {
    if (action === 'report') {
        showLoading();
        try {
            const res = await fetch('/api/get_report', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ tech: currentUser.tech })
            });
            const data = await res.json();
            if (data.status === 'success') showReportModal(data.html); else tg.showAlert(data.message);
        } catch (e) { tg.showAlert('Помилка'); } finally { hideLoading(); }
    } 
    else if (action === 'tasks') {
        showLoading();
        try {
            const res = await fetch('/api/get_tech_tasks', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ tech: currentUser.tech })
            });
            const data = await res.json();
            if (data.status === 'success') openTechTasksModal(data.tasks); else tg.showAlert(data.message);
        } catch (e) { tg.showAlert('Помилка'); } finally { hideLoading(); }
    }
    else if (action === 'orders') openOrderModal();
    else if (action === 'expenses') openExpensesModal();
}

// --- ТЕХДИРЕКТОР (ИСПРАВЛЕНО) ---
function handleTexdirAction(action) {
    switch (action) {
        // 1. Нове завдання (Как у Call-center)
        case 'new-task': 
            showProblemMenu(); 
            break;
        
        // 2. Нове замовлення карти (Как у Call-center)
        case 'new-card': 
            openCardOrderModal(); 
            break;
        
        // 3. Завдання з терміном (СВОЕ ОКНО)
        case 'termin-tasks': 
            openTerminModal(); 
            break;
        
        // 4. Стіл замовлень (Как у Sup1)
        case 'orders-table': 
            handleSuperAction('orders-table'); 
            break;
        
        // 5. Витрати (Как у Техника)
        case 'expenses': 
            openExpensesModal(); 
            break;
        
        // 6. Завдання та замовлення (Как у Техника)
        case 'all-tasks': 
            handleTechAction('tasks'); 
            break;
            
        default:
            console.log('Неизвестное действие:', action);
    }
}

// --- СУПЕРВИЗОР ---
async function handleSuperAction(action) {
    if (action === 'daily-report') {
        showLoading();
        try {
            const response = await fetch('/api/get_report', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ type: 'general', tech: 'super' })
            });
            const result = await response.json();
            if (result.status === 'success') showReportModal(result.html);
            else tg.showAlert('Помилка: ' + result.message);
        } catch (error) { tg.showAlert('Помилка з\'єднання'); } finally { hideLoading(); }
        return;
    }
    if (action === 'tasks-cards') {
        showLoading();
        try {
            const response = await fetch('/api/get_all_tasks', { method: 'POST' });
            const result = await response.json();
            if (result.status === 'success') openTasksModal(result.tasks);
            else tg.showAlert('Помилка: ' + result.message);
        } catch (e) { tg.showAlert('Помилка з\'єднання'); } finally { hideLoading(); }
        return;
    }
    // СТІЛ ЗАМОВЛЕНЬ (СУПЕРВИЗОР И ТЕХДИР)
    if (action === 'orders-table') {
        showLoading();
        try {
            const response = await fetch('/api/super/get_orders');
            const result = await response.json();
            if (result.status === 'success') openSuperOrdersModal(result.orders);
            else tg.showAlert(result.message);
        } catch(e) { tg.showAlert('Помилка'); } finally { hideLoading(); }
        return;
    }
    if (action === 'inki-5week') { runScript('week5'); return; }
    if (action === 'inki-1week') { runScript('week1'); return; }
    if (action === 'service-report') { runScript('service'); return; }

    const data = { action: action, role: 'super' };
    switch (action) {
        case 'map-file': sendToBot({ ...data, command: 'файл карты' }); break;
        case 'general-report': sendToBot({ ...data, command: '📉звіт' }); break;
        case 'service-big': sendToBot({ ...data, command: '📊service big звіт' }); break;
    }
}

async function runScript(type) {
    showLoading();
    try {
        const response = await fetch('/api/super/run_script', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ type: type })
        });
        const result = await response.json();
        if (result.status === 'success') showReportModal(result.html);
        else tg.showAlert('Помилка: ' + result.message);
    } catch (e) { tg.showAlert('Помилка з\'єднання'); } finally { hideLoading(); }
}

// --- ЛОГИКА "ЗАВДАННЯ З ТЕРМІНОМ" ---
function openTerminModal() {
    const modal = document.getElementById('termin-modal');
    document.getElementById('termin-text-input').value = '';
    document.getElementById('termin-days-input').value = '';
    document.getElementById('termin-addr-input').value = '';
    selectedTerminAparat = null;
    modal.classList.remove('hidden');
    // Настраиваем поиск
    setupSearch(document.getElementById('termin-addr-input'), 'termin-search-results', (item) => { selectedTerminAparat = item; });
}

function closeTerminModal() { document.getElementById('termin-modal').classList.add('hidden'); }

async function sendTerminToServer() {
    const text = document.getElementById('termin-text-input').value.trim();
    const days = document.getElementById('termin-days-input').value.trim();
    
    if (!text || !days || !selectedTerminAparat) {
        tg.showAlert('Заповніть всі поля!');
        return;
    }

    showLoading();
    try {
        const res = await fetch('/api/create_termin_task', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                id_terem: selectedTerminAparat.id,
                adres: selectedTerminAparat.addr,
                zavdanya: text,
                termin: days,
                // texnik определяется на сервере, но мы можем передать текущего юзера если нужно, но сервер сделает это сам
            })
        });
        const result = await res.json();
        if(result.status === 'success') { closeTerminModal(); tg.showAlert(result.message); }
        else alert(result.message);
    } catch(e) { alert('Помилка'); } finally { hideLoading(); }
}

// --- ФУНКЦИИ МОДАЛОК (ОСТАЛЬНЫЕ) ---

// Заказы (Sup + TexDir)
function openSuperOrdersModal(orders) {
    const modal = document.getElementById('report-modal');
    const content = document.getElementById('report-content');
    content.innerHTML = '<h3>🛒 Активні замовлення</h3><hr>';
    if (orders.length === 0) content.innerHTML += '<p style="text-align:center; color:#999">Замовлень немає</p>';
    else {
        orders.forEach(order => {
            content.innerHTML += `
                <div id="order-row-${order.id}" style="border-bottom:1px solid #eee; padding:10px; display:flex; justify-content:space-between; align-items:center;">
                    <div><div style="font-weight:bold;">${order.zakaz}</div><div style="font-size:12px; color:#555;">📍 ${order.adres}</div><div style="font-size:11px; color:#999;">👤 ${order.texnik} | 📅 ${order.date}</div></div>
                    <button class="btn-primary" style="padding:5px 10px; font-size:12px; background:#3498db;" onclick="closeSuperOrder(${order.id})">Замовив</button>
                </div>`;
        });
    }
    modal.classList.remove('hidden');
}

async function closeSuperOrder(orderId) {
    if(!confirm('Замовлено?')) return;
    try {
        const res = await fetch('/api/super/close_order', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ id: orderId })
        });
        const result = await res.json();
        if(result.status === 'success') { document.getElementById(`order-row-${orderId}`).remove(); tg.showAlert('Статус змінено!'); }
    } catch(e) { alert('Помилка'); }
}

// Задачи Техника
function openTechTasksModal(tasks) {
    const modal = document.getElementById('tech-tasks-modal');
    const list = document.getElementById('tech-tasks-list');
    list.innerHTML = '';
    if (tasks.length === 0) list.innerHTML = '<div style="text-align:center; padding:20px; color:#999">Немає завдань 🎉</div>';
    else {
        tasks.forEach(task => {
            const displayText = `${task.icon} ${task.info} ${task.icon} ${task.adres}`;
            let bgStyle = 'border-left: 4px solid #f39c12;';
            if (task.type === 'urgent') bgStyle = 'border-left: 4px solid #e74c3c; background: rgba(231,76,60,0.1);';
            if (task.type === 'order') bgStyle = 'border-left: 4px solid #3498db; background: rgba(52,152,219,0.1);';
            list.innerHTML += `
                <div class="task-item" style="${bgStyle} padding:12px; margin-bottom:10px; border-radius:8px;">
                    <div style="font-weight:600; font-size:15px; margin-bottom:8px;">${displayText}</div>
                    <div style="font-size:12px; color:#777; margin-bottom:10px;">Створено: ${task.date}</div>
                    <button class="btn-primary" style="background:#27ae60; padding:8px; font-size:14px; width:100%" onclick="completeTask(${task.id}, '${task.table}')">✅ Виконано</button>
                </div>`;
        });
    }
    modal.classList.remove('hidden');
}

async function completeTask(taskId, tableName) {
    if(!confirm('Виконано?')) return;
    showLoading();
    try {
        const res = await fetch('/api/complete_task', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ task_id: taskId, table: tableName })
        });
        const result = await res.json();
        if(result.status === 'success') { document.getElementById('tech-tasks-modal').classList.add('hidden'); tg.showAlert('Готово!'); }
    } catch(e) { alert('Помилка'); } finally { hideLoading(); }
}

function closeTechTasksModal() { document.getElementById('tech-tasks-modal').classList.add('hidden'); }

// 3. ОКНА ВВОДА (Карты, Срочно, Расходы, Заказы)
function openCardOrderModal() {
    const modal = document.getElementById('card-order-modal');
    document.getElementById('card-name-input').value = ''; document.getElementById('card-addr-input').value = '';
    selectedCardAparat = null; modal.classList.remove('hidden');
    setupSearch(document.getElementById('card-addr-input'), 'card-search-results', (item) => { selectedCardAparat = item; });
}
async function sendCardOrderToServer() {
    const name = document.getElementById('card-name-input').value.trim();
    if (!name || !selectedCardAparat) { tg.showAlert('Заповніть поля!'); return; }
    showLoading();
    try {
        const res = await fetch('/api/create_card_order', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ id_terem: selectedCardAparat.id, adres: selectedCardAparat.addr, zamovnuk: name, texnik: selectedCardAparat.tech })
        });
        if((await res.json()).status === 'success') { document.getElementById('card-order-modal').classList.add('hidden'); tg.showAlert('Замовлено!'); }
    } catch(e) { alert('Помилка'); } finally { hideLoading(); }
}
function closeCardOrderModal() { document.getElementById('card-order-modal').classList.add('hidden'); }

function openUrgentModal() {
    const modal = document.getElementById('urgent-modal');
    document.getElementById('urgent-reason-input').value = ''; document.getElementById('urgent-addr-input').value = '';
    selectedUrgentAparat = null; modal.classList.remove('hidden');
    setupSearch(document.getElementById('urgent-addr-input'), 'urgent-search-results', (item) => { selectedUrgentAparat = item; });
}
async function sendUrgentToServer() {
    const reason = document.getElementById('urgent-reason-input').value.trim();
    if (!reason || !selectedUrgentAparat) { tg.showAlert('Заповніть поля!'); return; }
    showLoading();
    try {
        const res = await fetch('/api/create_urgent_task', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ id_terem: selectedUrgentAparat.id, adres: selectedUrgentAparat.addr, pricina: reason, texnik: selectedUrgentAparat.tech })
        });
        if((await res.json()).status === 'success') { document.getElementById('urgent-modal').classList.add('hidden'); tg.showAlert('Відправлено!'); }
    } catch(e) { alert('Помилка'); } finally { hideLoading(); }
}
function closeUrgentModal() { document.getElementById('urgent-modal').classList.add('hidden'); }

function openOrderModal() {
    const modal = document.getElementById('order-modal');
    document.getElementById('order-name-input').value = ''; document.getElementById('order-address-input').value = '';
    modal.classList.remove('hidden');
    const input = document.getElementById('order-address-input');
    setupSearch(input, 'order-search-results', (item) => { input.value = item.addr; });
}
async function sendOrderToServer() {
    const name = document.getElementById('order-name-input').value.trim(); const address = document.getElementById('order-address-input').value.trim();
    if(!name || !address) { tg.showAlert('Заповніть поля'); return; }
    showLoading();
    try {
        const res = await fetch('/api/create_order', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ texnik: currentUser.tech, adres: address, zakaz: name })
        });
        if((await res.json()).status === 'success') { document.getElementById('order-modal').classList.add('hidden'); tg.showAlert('Замовлено!'); }
    } catch(e) { alert('Помилка'); } finally { hideLoading(); }
}
function closeOrderModal() { document.getElementById('order-modal').classList.add('hidden'); }

function openExpensesModal() {
    document.getElementById('expenses-modal').classList.remove('hidden');
    document.getElementById('expense-name-input').value = ''; document.getElementById('expense-sum-input').value = '';
}
async function sendExpenseToServer() {
    const name = document.getElementById('expense-name-input').value; const sum = document.getElementById('expense-sum-input').value;
    if(!name || !sum) { tg.showAlert('Заповніть поля'); return; }
    showLoading();
    try {
        const res = await fetch('/api/create_expense', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ texnik: currentUser.tech, vutratu: name, summa: sum })
        });
        if((await res.json()).status === 'success') { document.getElementById('expenses-modal').classList.add('hidden'); tg.showAlert('Додано!'); }
    } catch(e) { alert('Помилка'); } finally { hideLoading(); }
}
function closeExpensesModal() { document.getElementById('expenses-modal').classList.add('hidden'); }

// 4. СТАТУС (ВСЕ ЗАДАЧИ)
async function openStatusModal() {
    const modal = document.getElementById('status-modal');
    const list = document.getElementById('status-list');
    modal.classList.remove('hidden');
    list.innerHTML = 'Завантаження...';
    try {
        const res = await fetch('/api/get_all_active_tasks');
        const data = await res.json();
        list.innerHTML = '';
        if (data.tasks.length === 0) list.innerHTML = '<div style="text-align:center; padding:20px; color:#999">Немає активних завдань</div>';
        data.tasks.forEach(task => {
            let icon = '📝'; if (task.type === 'card') icon = '💳'; if (task.type === 'urgent') icon = '🔥';
            list.innerHTML += `
                <div class="task-item" id="status-card-${task.table}-${task.id}" style="display:flex; justify-content:space-between; align-items:center;">
                    <div style="flex-grow:1;"><div style="font-weight:bold;">${icon} ${task.adres}</div><div style="font-size:13px;">${task.info}</div><div style="font-size:11px; color:#999;">${task.who}</div></div>
                    <button class="btn-secondary" style="background:#e74c3c; color:white; padding:8px 12px; font-size:12px; margin-left:10px;" onclick="cancelTask('${task.table}', ${task.id})">Відміна</button>
                </div>`;
        });
    } catch(e) { list.innerHTML = 'Помилка'; }
}
function closeStatusModal() { document.getElementById('status-modal').classList.add('hidden'); }
async function cancelTask(table, id) {
    if(!confirm('Скасувати?')) return;
    try {
        const res = await fetch('/api/cancel_task', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ table: table, id: id })
        });
        if((await res.json()).status === 'success') document.getElementById(`status-card-${table}-${id}`).remove();
    } catch(e) { alert('Помилка'); }
}

// 5. ЗАДАЧИ СУПЕРВИЗОРА (ВКЛАДКИ)
function openTasksModal(tasks) {
    const modal = document.getElementById('tasks-modal');
    const listOpen = document.getElementById('list-open');
    const listClosed = document.getElementById('list-closed');
    listOpen.innerHTML = ''; listClosed.innerHTML = '';
    let openCount = 0;
    tasks.forEach(task => {
        const html = `
            <div class="task-item status-${task.status}">
                <div class="task-row"><span class="task-addr">[${task.id_terem}] ${task.adres}</span><span class="task-tech">${task.texnik}</span></div>
                <div class="task-problem">${task.zadaca}</div><div class="task-date">📅 ${task.date}</div>
            </div>`;
        if (task.status === 'open') { listOpen.innerHTML += html; openCount++; } else listClosed.innerHTML += html;
    });
    if (openCount === 0) listOpen.innerHTML = '<p style="text-align:center; padding:20px; color:#999">Немає</p>';
    modal.classList.remove('hidden');
    switchTab('open');
}
function closeTasksModal() { document.getElementById('tasks-modal').classList.add('hidden'); }
function switchTab(tabName) {
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
    if(event && event.target) event.target.classList.add('active'); else document.querySelector('.tab-btn').classList.add('active');
    document.getElementById('tab-' + tabName).classList.add('active');
}

// Служебные
function showReportModal(html) { document.getElementById('report-modal').classList.remove('hidden'); document.getElementById('report-content').innerHTML = html; }
function closeReportModal() { document.getElementById('report-modal').classList.add('hidden'); }
function sendToBot(data) { tg.sendData(JSON.stringify(data)); }
function showLoading() { document.getElementById('loading').classList.remove('hidden'); }
function hideLoading() { document.getElementById('loading').classList.add('hidden'); }
function showTechCabinet() { document.getElementById('tech-name').innerText = currentUser.name; document.getElementById('tech-cabinet').classList.add('active'); }
function showCallCenterCabinet() { document.getElementById('callcenter-cabinet').classList.add('active'); }
function showTexdirCabinet() { document.getElementById('texdir-cabinet').classList.add('active'); }
function showSuperCabinet() { document.getElementById('super-cabinet').classList.add('active'); }
function handleReportHelp() { tg.showAlert('Help!'); }
function logout() { location.reload(); }

// События
tg.onEvent('mainButtonClicked', logout);
document.querySelectorAll('.modal').forEach(m => m.addEventListener('click', e => { if(e.target === m) m.classList.add('hidden'); }));
document.getElementById('problem-menu').addEventListener('click', e => { if(e.target.id === 'problem-menu') e.target.classList.add('hidden'); });
document.querySelectorAll('.problem-btn').forEach(btn => btn.onclick = () => {
    currentProblem = btn.dataset.problem;
    document.getElementById('problem-menu').classList.add('hidden');
    openAddressModal(currentProblem);
});
function openAddressModal(problem) {
    document.getElementById('address-modal').classList.remove('hidden');
    document.getElementById('selected-problem-name').innerText = problem;
    document.getElementById('address-input').value = '';
    selectedAparat = null;
    setupSearch(document.getElementById('address-input'), 'search-results', selectAddress);
}
function closeAddressModal() { document.getElementById('address-modal').classList.add('hidden'); }
document.getElementById('password-input').addEventListener('keypress', e => { if(e.key === 'Enter') handleLogin(); });

console.log('App Ready');