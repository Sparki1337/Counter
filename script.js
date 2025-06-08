document.addEventListener('DOMContentLoaded', () => {
    const MAX_MESSAGES = 6;
    const SIMILARITY_THRESHOLD = 0.9;
    const STORAGE_KEY = 'counter_app_state';

    const dataInput = document.getElementById('dataInput');
    const submitBtn = document.getElementById('submitBtn');
    const clearLastBtn = document.getElementById('clearLastBtn');
    const newCountBtn = document.getElementById('newCountBtn');
    const resultsDiv = document.getElementById('results');
    const progressBar = document.getElementById('progressBar');
    const progressText = document.getElementById('progressText');
    const statusMessage = document.getElementById('statusMessage');
    
    const instructionBtn = document.getElementById('instructionBtn');
    const instructionModal = document.getElementById('instructionModal');
    const closeModalBtn = document.querySelector('.close-btn');
    const pasteFromClipboardBtn = document.getElementById('pasteFromClipboardBtn');

    function applyButtonStyles() {
        const buttons = document.querySelectorAll('.controls-secondary button');
        const commonStyle = {
            backgroundColor: '#424242',
            color: '#aaa',
            border: '1px solid #555',
            fontWeight: '500',
            boxShadow: 'none',
            textShadow: 'none'
        };
        
        buttons.forEach(button => {
            Object.assign(button.style, commonStyle);
        });
    }
    
    applyButtonStyles();

    let state = {
        count: 0,
        values: {},
        last_additions: [],
    };

    function loadState() {
        const savedState = localStorage.getItem(STORAGE_KEY);
        if (savedState) {
            state = JSON.parse(savedState);
        }
        updateUI();
    }

    function saveState() {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    }

    function updateUI() {
        let resultText = '';
        const sortedKeys = Object.keys(state.values);

        if (sortedKeys.length === 0) {
            resultText = 'Пока нет данных...';
        } else {
            for (const key of sortedKeys) {
                resultText += `${key} - ${state.values[key]}\n`;
            }
        }
        resultsDiv.textContent = resultText.trim();

        const progressPercentage = state.count > 0 ? (state.count / MAX_MESSAGES) * 100 : 0;
        progressBar.style.width = `${progressPercentage}%`;
        progressText.textContent = `Обработано: ${state.count} из ${MAX_MESSAGES}`;
        
        statusMessage.textContent = '';
    }
    
    function stringSimilarity(s1, s2) {
        let longer = s1;
        let shorter = s2;
        if (s1.length < s2.length) {
            longer = s2;
            shorter = s1;
        }
        const longerLength = longer.length;
        if (longerLength === 0) {
            return 1.0;
        }
        return (longerLength - editDistance(longer, shorter)) / parseFloat(longerLength);
    }

    function editDistance(s1, s2) {
        s1 = s1.toLowerCase();
        s2 = s2.toLowerCase();

        const costs = [];
        for (let i = 0; i <= s1.length; i++) {
            let lastValue = i;
            for (let j = 0; j <= s2.length; j++) {
                if (i === 0) {
                    costs[j] = j;
                } else {
                    if (j > 0) {
                        let newValue = costs[j - 1];
                        if (s1.charAt(i - 1) !== s2.charAt(j - 1)) {
                            newValue = Math.min(Math.min(newValue, lastValue), costs[j]) + 1;
                        }
                        costs[j - 1] = lastValue;
                        lastValue = newValue;
                    }
                }
            }
            if (i > 0) costs[s2.length] = lastValue;
        }
        return costs[s2.length];
    }
    
    function removeTrailingLetters(text) {
        return text.replace(/(\d+[.,]?\d*)[а-яА-Яa-zA-Z]+\b/g, '$1');
    }

    function normalizeCategoryName(name) {
        let normalized = name.trim().replace(/\s+/g, ' ');
        normalized = removeTrailingLetters(normalized);
        return normalized;
    }
    
    function findSimilarCategory(name, values) {
        const normalizedName = normalizeCategoryName(name);
        
        for (const existingName in values) {
            if (normalizeCategoryName(existingName) === normalizedName) {
                return existingName;
            }
        }
        
        let bestMatch = null;
        let highestSimilarity = 0.0;

        for (const existingName in values) {
            const similarity = stringSimilarity(normalizedName, normalizeCategoryName(existingName));
            if (similarity >= SIMILARITY_THRESHOLD && similarity > highestSimilarity) {
                highestSimilarity = similarity;
                bestMatch = existingName;
            }
        }
        
        return bestMatch || name;
    }
    
    function parseLine(line) {
        const separator = line.includes(':') ? ':' : '-';
        const parts = line.split(separator);

        if (parts.length < 2) return null;

        const name = parts[0].trim();
        const valuePart = parts.slice(1).join(separator).trim();
        const match = valuePart.match(/-?\d+/);
        
        if (name && match) {
            return { name, value: parseInt(match[0], 10) };
        }
        
        return null;
    }

    submitBtn.addEventListener('click', () => {
        const text = dataInput.value.trim();
        if (!text) {
            statusMessage.textContent = 'Поле ввода пустое.';
            return;
        }

        if (state.count >= MAX_MESSAGES) {
            if (confirm(`Достигнут лимит в ${MAX_MESSAGES} сообщений. Начать новый цикл подсчета? (суммы будут сброшены)`)) {
                handleNewCount();
            } else {
                return;
            }
        }

        const lines = text.split('\n');
        const currentAdditions = [];

        for (const line of lines) {
            if (line.trim() === '') continue;

            const parsed = parseLine(line);
            if (parsed) {
                const finalName = findSimilarCategory(parsed.name, state.values);
                
                state.values[finalName] = (state.values[finalName] || 0) + parsed.value;
                currentAdditions.push({ name: finalName, value: parsed.value });
            }
        }
        
        if (currentAdditions.length > 0) {
            state.last_additions = currentAdditions;
            state.count++;
            dataInput.value = '';
        } else {
            statusMessage.textContent = 'Не удалось найти данные для обработки. Проверьте формат.';
        }

        updateUI();
        saveState();
    });

    clearLastBtn.addEventListener('click', () => {
        if (state.last_additions.length === 0) {
            statusMessage.textContent = 'Нет данных для отмены.';
            return;
        }
        
        for(const addition of state.last_additions) {
            if(state.values[addition.name] !== undefined) {
                state.values[addition.name] -= addition.value;
                if (state.values[addition.name] === 0 && Object.keys(state.values).length > 1) {
                }
            }
        }

        state.last_additions.forEach(addition => {
            if (state.values[addition.name] === 0) {
                let wasPresentBefore = false;
                for (let i = 0; i < state.last_additions.length; i++) {
                     if(state.last_additions[i].name === addition.name && state.last_additions[i] !== addition) {
                         wasPresentBefore = true;
                         break;
                     }
                }
                if (!wasPresentBefore) {
                }
            }
        });


        state.count--;
        state.last_additions = [];

        if (Object.values(state.values).every(v => v === 0)) {
            state.values = {};
        }

        updateUI();
        saveState();
    });

    function handleNewCount() {
        state.count = 0;
        state.values = {};
        state.last_additions = [];
        updateUI();
        saveState();
    }
    
    newCountBtn.addEventListener('click', () => {
        if (confirm('Вы уверены, что хотите начать новый подсчет? Все текущие суммы будут удалены.')) {
            handleNewCount();
        }
    });

    instructionBtn.addEventListener('click', () => {
        instructionModal.style.display = 'block';
    });

    closeModalBtn.addEventListener('click', () => {
        instructionModal.style.display = 'none';
    });

    window.addEventListener('click', (event) => {
        if (event.target == instructionModal) {
            instructionModal.style.display = 'none';
        }
    });

    pasteFromClipboardBtn.addEventListener('click', async () => {
        try {
            if (navigator.clipboard && navigator.clipboard.readText) {
                const text = await navigator.clipboard.readText();
                dataInput.value = text;
                statusMessage.textContent = 'Текст вставлен из буфера обмена.';
            } else {
                fallbackPaste();
            }
        } catch (err) {
            console.error('Clipboard error:', err);
            statusMessage.textContent = `Не удалось вставить текст: ${err.message || 'нет доступа к буферу обмена'}`;
            fallbackPaste();
        }
    });
    
    function fallbackPaste() {
        try {
            const tempInput = document.createElement('textarea');
            tempInput.style.position = 'fixed';
            tempInput.style.opacity = '0';
            document.body.appendChild(tempInput);
            tempInput.focus();
            
            const successful = document.execCommand('paste');
            
            if (successful) {
                dataInput.value = tempInput.value;
                statusMessage.textContent = 'Текст вставлен из буфера обмена.';
            } else {
                statusMessage.textContent = 'Для вставки текста используйте комбинацию клавиш Ctrl+V';
                dataInput.focus();
            }
            
            document.body.removeChild(tempInput);
        } catch (err) {
            console.error('Fallback paste error:', err);
            statusMessage.textContent = 'Для вставки текста используйте комбинацию клавиш Ctrl+V';
            dataInput.focus();
        }
    }
    
    dataInput.addEventListener('keydown', function(event) {
        if ((event.ctrlKey || event.metaKey) && event.key === 'v') {
            statusMessage.textContent = 'Текст вставлен с помощью клавиш.';
        }
    });
    
    loadState();
});