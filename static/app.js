// Инициализация Telegram Web App (fallback for local testing)
let tg = (function() {
  // If Telegram Web App is available, use it
  if (window.Telegram && window.Telegram.WebApp) {
    return window.Telegram.WebApp;
  }
  // Fallback mock object with minimal methods used in the app
  return {
    expand: function() {},
    enableClosingConfirmation: function() {},
    showAlert: function(msg) { alert(msg); },
    // Add any other methods you might call later
  };
})();
// Ensure the mock methods exist
if (typeof tg.expand !== 'function') tg.expand = function() {};
if (typeof tg.enableClosingConfirmation !== 'function') tg.enableClosingConfirmation = function() {};
if (typeof tg.showAlert !== 'function') tg.showAlert = function(msg) { alert(msg); };

// Тема
document.documentElement.style.setProperty('--tg-theme-bg-color', tg.themeParams.bg_color || '#ffffff');
document.documentElement.style.setProperty('--tg-theme-text-color', tg.themeParams.text_color || '#000000');
document.documentElement.style.setProperty('--tg-theme-hint-color', tg.themeParams.hint_color || '#999999');
document.documentElement.style.setProperty('--tg-theme-button-color', tg.themeParams.button_color || '#5288c1');
document.documentElement.style.setProperty('--tg-theme-button-text-color', tg.themeParams.button_text_color || '#ffffff');
document.documentElement.style.setProperty('--tg-theme-secondary-bg-color', tg.themeParams.secondary_bg_color || '#f4f4f5');
document.documentElement.style.setProperty('--primary-color', tg.themeParams.button_color || '#5288c1'); // Добавим для удобства

// Пользователи
const users = {
    'sup1': { password: 'sup1$', role: 'super', name: 'Супервізор' },
    'rus1': { password: 'rus1$', role: 'tech', name: 'Руслан', tech: 'ruslan' },
    'callcentr1': { password: 'callcentr1$', role: 'callcenter', name: 'Call-центр' },
    'igor1': { password: 'igor1$', role: 'tech', name: 'Ігор', tech: 'igor' },
    'dmut1': { password: 'dmut1$', role: 'tech', name: 'Дмитро', tech: 'dmutro' },
    'texdir1': { password: 'texdir1$', role: 'texdir', name: 'Техдиректор', tech: 'texdir' } 
};

let currentUser = null;
let allAddresses = []; 
let selectedAparat = null; 
let currentProblem = null;
let selectedCardAparat = null;
let selectedUrgentAparat = null;
let selectedTerminAparat = null;

// ... (Остальной код загрузки, setupSearch, selectAddress и т.д. остается без изменений) ...

// ==========================================================
// ОСНОВНЫЕ ФУНКЦИИ АВТОПРОГРУЗКИ (НОВЫЕ/ИЗМЕНЕННЫЕ)
// ==========================================================

/**
 * Функция для получения данных о заработке и обновлении дашборда.
 * АВТОПРОГРУЗКА
 */
function loadSupervisorData() {
    const spinner = document.getElementById('earnings-spinner');
    const earningsData = document.getElementById('earnings-data');
    const cashDisplay = document.getElementById('cash-earnings');
    const noncashDisplay = document.getElementById('noncash-earnings');
    const totalDisplay = document.getElementById('total-earnings-display');
    const timeDisplay = document.getElementById('current-time-display');

    // 1. Показываем спиннер, скрываем данные
    if (spinner) spinner.classList.add('active');
    if (earningsData) earningsData.classList.add('hidden');
    
    // 2. Отправляем запрос на сервер
    fetch('/api/super_earnings')
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => {
            if (data.status === 'ok') {
                // 3. Обновляем данные на странице
                if (cashDisplay) cashDisplay.innerText = data.cash;
                if (noncashDisplay) noncashDisplay.innerText = data.noncash;
                if (timeDisplay) timeDisplay.innerText = data.time;

                // Вычисляем и отображаем общий доход
                const cashValue = parseFloat(String(data.cash).replace(' UAH', '').replace(/,/g, ''));
                const noncashValue = parseFloat(String(data.noncash).replace(' UAH', '').replace(/,/g, ''));
                const totalValue = (cashValue + noncashValue).toLocaleString('uk-UA', { 
                    minimumFractionDigits: 2, 
                    maximumFractionDigits: 2 
                });
                if (totalDisplay) totalDisplay.innerText = `${totalValue} UAH`;

                // 4. Скрываем спиннер, показываем данные
                if (spinner) spinner.classList.remove('active');
                if (earningsData) earningsData.classList.remove('hidden');
                
            } else {
                console.error('Ошибка API:', data.message);
                if (earningsData) earningsData.innerHTML = `<p style="color: var(--danger-color);">Помилка: ${data.message}</p>`;
                if (spinner) spinner.classList.remove('active');
                if (earningsData) earningsData.classList.remove('hidden');
            }
        })
        .catch(error => {
            console.error('Fetch error:', error);
            if (earningsData) earningsData.innerHTML = `<p style="color: var(--danger-color);">Не вдалося завантажити дані.</p>`;
            if (spinner) spinner.classList.remove('active');
            if (earningsData) earningsData.classList.remove('hidden');
        });
}

/**
 * Функция показа кабинета Супервизора, которая вызывает автопрогрузку дашборда
 */
function showSuperCabinet() { 
    document.getElementById('super-cabinet').classList.add('active'); 
    // АВТОПРОГРУЗКА СКРИПТА ДЛЯ ДАШБОРДА
    loadSupervisorData(); 
}


// --- Вход (handleLogin) ---
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
            // ЗАПУСКАЕМ ЭКСПОРТ CSV В ФОНОВОМ РЕЖИМЕ
            fetch('/api/super/export_mes_csv', { method: 'POST' }).catch(console.error);
            break;
    }
    sendToBot({ action: 'login', role: currentUser.role, name: currentUser.name });
}

// ... (Остальные функции: showTechCabinet, showCallCenterCabinet, handleCallCenterAction, handleTechAction и т.д. остаются без изменений) ...

// ... (Весь ваш код модальных окон, вспомогательных функций и обработчиков событий остается без изменений) ...