/**
 * DK/400 - IBM 5250 Terminal Emulator
 * Full-screen WebSocket-based terminal with authentic AS/400 feel
 */

class Terminal5250 {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.ws = null;
        this.screenData = [];
        this.currentScreen = 'signon';
        this.inputFields = [];
        this.activeFieldIndex = 0;
        this.connected = false;
        this.sessionEnded = false;

        // Screen dimensions
        this.cols = 80;
        this.rows = 24;

        // System busy indicator
        this.busyStartTime = null;
        this.busyTimerInterval = null;

        this.init();
    }

    init() {
        this.render();
        this.connect();
        this.setupKeyboardHandler();
        this.setupResizeHandler();
        this.adjustFontSize();
    }

    setupResizeHandler() {
        window.addEventListener('resize', () => this.adjustFontSize());
    }

    adjustFontSize() {
        const screen = this.container.querySelector('.terminal-screen');
        if (!screen) return;

        const cols = this.cols || 80;
        const rows = 27; // 24 content + 3 for function keys and padding

        // Calculate available space (with some margin)
        const availWidth = window.innerWidth * 0.98;
        const availHeight = window.innerHeight * 0.98;

        // Calculate font size based on character cell dimensions
        // A monospace character is roughly 0.6 times as wide as it is tall
        const charAspect = 0.6;

        // Calculate font size to fit width
        const fontByWidth = availWidth / (cols * charAspect);

        // Calculate font size to fit height (line-height is 1.2em)
        const fontByHeight = availHeight / (rows * 1.2);

        // Use the smaller of the two to ensure it fits
        const fontSize = Math.floor(Math.min(fontByWidth, fontByHeight));

        // Clamp to reasonable range
        const clampedSize = Math.max(10, Math.min(fontSize, 32));

        screen.style.fontSize = `${clampedSize}px`;
    }

    connect() {
        if (this.sessionEnded) return;

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws`;

        this.showMessage('Connecting to DK/400...');

        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
            this.connected = true;
            this.showMessage('Connected');
            this.send({ action: 'init' });
        };

        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.handleMessage(data);
        };

        this.ws.onclose = () => {
            this.connected = false;
            if (!this.sessionEnded) {
                this.showMessage('Connection lost. Reconnecting...');
                setTimeout(() => this.connect(), 3000);
            }
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };
    }

    send(data) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(data));
        }
    }

    handleMessage(data) {
        // Hide busy indicator on any response
        this.hideBusy();

        switch (data.type) {
            case 'screen':
                this.updateScreen(data);
                break;
            case 'message':
                this.showStatusMessage(data.text, data.level);
                break;
            case 'exit':
                this.exitSession();
                break;
            case 'bell':
                this.bell();
                break;
        }
    }

    updateScreen(data) {
        this.currentScreen = data.screen || this.currentScreen;
        this.screenData = data.content || [];
        this.inputFields = data.fields || [];
        this.activeFieldIndex = data.activeField || 0;

        const oldCols = this.cols;
        this.cols = data.cols || 80;

        // Apply screen width class
        const screen = this.container.querySelector('.terminal-screen');
        if (screen) {
            screen.classList.remove('cols-80', 'cols-132');
            screen.classList.add(`cols-${this.cols}`);
        }

        // Recalculate font size if columns changed
        if (oldCols !== this.cols) {
            this.adjustFontSize();
        }

        this.renderScreen();

        // Focus the active input field
        if (this.inputFields.length > 0) {
            setTimeout(() => this.focusField(this.activeFieldIndex), 50);
        }

        this.refreshEffect();
    }

    renderScreen() {
        const content = this.container.querySelector('.screen-content');
        if (!content) return;

        let html = '';

        this.screenData.forEach((row, rowIndex) => {
            html += `<div class="screen-row" data-row="${rowIndex}">`;

            if (typeof row === 'string') {
                html += this.escapeHtml(row);
            } else if (Array.isArray(row)) {
                row.forEach(segment => {
                    if (segment.type === 'text') {
                        const cssClass = segment.class || 'field-output';
                        html += `<span class="${cssClass}">${this.escapeHtml(segment.text)}</span>`;
                    } else if (segment.type === 'input') {
                        const fieldId = `field-${segment.id}`;
                        const value = segment.value || '';
                        const width = segment.width || 10;
                        const maxLength = segment.maxLength || width;
                        const cssClass = segment.class || 'field-input';
                        const inputType = segment.password ? 'password' : 'text';

                        html += `<input
                            id="${fieldId}"
                            class="input-field ${cssClass}"
                            type="${inputType}"
                            value="${this.escapeHtml(value)}"
                            maxlength="${maxLength}"
                            style="width: ${width}ch;"
                            data-field-id="${segment.id}"
                            autocomplete="off"
                            autocorrect="off"
                            autocapitalize="off"
                            spellcheck="false"
                        >`;
                    }
                });
            }

            html += '</div>';
        });

        content.innerHTML = html;
        this.attachFieldListeners();
    }

    attachFieldListeners() {
        const inputs = this.container.querySelectorAll('.input-field');

        inputs.forEach((input, index) => {
            input.addEventListener('keydown', (e) => this.handleFieldKeydown(e, index));
            input.addEventListener('focus', () => {
                this.activeFieldIndex = index;
            });
        });
    }

    handleFieldKeydown(e, fieldIndex) {
        switch (e.key) {
            case 'Enter':
                e.preventDefault();
                if (e.shiftKey) {
                    // Shift+Enter = Field Exit (move to next field)
                    this.focusNextField();
                } else {
                    // Enter = Submit screen
                    this.submitScreen();
                }
                break;

            case 'Tab':
                e.preventDefault();
                if (e.shiftKey) {
                    this.focusPreviousField();
                } else {
                    this.focusNextField();
                }
                break;

            case 'F1':
            case 'F2':
            case 'F3':
            case 'F4':
            case 'F5':
            case 'F6':
            case 'F7':
            case 'F8':
            case 'F9':
            case 'F10':
            case 'F11':
            case 'F12':
                e.preventDefault();
                this.handleFunctionKey(e.key);
                break;

            case 'Escape':
                e.preventDefault();
                this.handleFunctionKey('F12');
                break;

            case 'PageUp':
                e.preventDefault();
                this.handleRoll('up');
                break;

            case 'PageDown':
                e.preventDefault();
                this.handleRoll('down');
                break;

            case 'ArrowUp':
                // Cmd+Up or Opt+Up = Roll Up
                if (e.metaKey || e.altKey) {
                    e.preventDefault();
                    this.handleRoll('up');
                } else {
                    e.preventDefault();
                    this.focusPreviousField();
                }
                break;

            case 'ArrowDown':
                // Cmd+Down or Opt+Down = Roll Down
                if (e.metaKey || e.altKey) {
                    e.preventDefault();
                    this.handleRoll('down');
                } else {
                    e.preventDefault();
                    this.focusNextField();
                }
                break;
        }
    }

    focusField(index) {
        const inputs = this.container.querySelectorAll('.input-field');
        if (inputs[index]) {
            inputs[index].focus();
            this.activeFieldIndex = index;
        }
    }

    focusNextField() {
        const inputs = this.container.querySelectorAll('.input-field');
        if (inputs.length > 0) {
            const nextIndex = (this.activeFieldIndex + 1) % inputs.length;
            this.focusField(nextIndex);
        }
    }

    focusPreviousField() {
        const inputs = this.container.querySelectorAll('.input-field');
        if (inputs.length > 0) {
            const prevIndex = (this.activeFieldIndex - 1 + inputs.length) % inputs.length;
            this.focusField(prevIndex);
        }
    }

    submitScreen() {
        const fieldValues = {};
        const inputs = this.container.querySelectorAll('.input-field');

        inputs.forEach(input => {
            const fieldId = input.dataset.fieldId;
            fieldValues[fieldId] = input.value;
        });

        this.showBusy();
        this.send({
            action: 'submit',
            screen: this.currentScreen,
            fields: fieldValues
        });
    }

    handleFunctionKey(key) {
        // F3 on sign-on screen = exit/close browser
        if (key === 'F3' && this.currentScreen === 'signon') {
            this.exitSession();
            return;
        }

        const fieldValues = {};
        const inputs = this.container.querySelectorAll('.input-field');
        inputs.forEach(input => {
            const fieldId = input.dataset.fieldId;
            fieldValues[fieldId] = input.value;
        });

        // Include the currently focused field for F4 parameter prompts
        const activeElement = document.activeElement;
        console.log('F4 DEBUG: activeElement=', activeElement?.id, 'classList=', activeElement?.classList, 'fieldId=', activeElement?.dataset?.fieldId);
        if (activeElement && activeElement.classList.contains('input-field')) {
            fieldValues['_active_field'] = activeElement.dataset.fieldId || '';
            console.log('F4 DEBUG: Set _active_field to', fieldValues['_active_field']);
        } else {
            console.log('F4 DEBUG: No input-field focused');
        }

        this.showBusy();
        this.send({
            action: 'function_key',
            key: key,
            screen: this.currentScreen,
            fields: fieldValues
        });
    }

    handleRoll(direction) {
        this.showBusy();
        this.send({
            action: 'roll',
            direction: direction,
            screen: this.currentScreen
        });
    }

    exitSession() {
        this.sessionEnded = true;

        // Close WebSocket
        if (this.ws) {
            this.ws.close();
        }

        // Show session ended screen briefly, then close
        this.container.innerHTML = `
            <div class="session-ended">
                <h1>Session Ended</h1>
                <p>You have signed off from DK/400</p>
            </div>
            <div class="scanlines"></div>
            <div class="vignette"></div>
        `;

        // Try to close the window/tab after a brief delay
        setTimeout(() => {
            // Try window.close() - works if we opened this tab via script
            window.close();

            // If window.close() didn't work (user opened directly),
            // navigate to about:blank
            setTimeout(() => {
                window.location.href = 'about:blank';
            }, 500);
        }, 1500);
    }

    setupKeyboardHandler() {
        document.addEventListener('keydown', (e) => {
            // Roll keys work globally (even when input focused)
            if (e.key === 'PageUp') {
                e.preventDefault();
                this.handleRoll('up');
                return;
            }
            if (e.key === 'PageDown') {
                e.preventDefault();
                this.handleRoll('down');
                return;
            }
            // Cmd/Opt + Arrow for roll
            if ((e.metaKey || e.altKey) && e.key === 'ArrowUp') {
                e.preventDefault();
                this.handleRoll('up');
                return;
            }
            if ((e.metaKey || e.altKey) && e.key === 'ArrowDown') {
                e.preventDefault();
                this.handleRoll('down');
                return;
            }

            // If an input field is focused, let it handle other events
            if (document.activeElement.classList.contains('input-field')) {
                return;
            }

            // Handle function keys globally
            if (e.key.startsWith('F') && e.key.length <= 3) {
                e.preventDefault();
                this.handleFunctionKey(e.key);
                return;
            }

            if (e.key === 'Escape') {
                e.preventDefault();
                this.handleFunctionKey('F12');
                return;
            }

            // Any printable character - focus first input
            if (e.key.length === 1 && !e.ctrlKey && !e.altKey && !e.metaKey) {
                const firstInput = this.container.querySelector('.input-field');
                if (firstInput) {
                    firstInput.focus();
                }
            }
        });

        // Handle clicks on function key labels
        this.container.addEventListener('click', (e) => {
            if (e.target.classList.contains('fkey')) {
                const key = e.target.dataset.key;
                if (key) {
                    this.handleFunctionKey(key);
                }
            }
        });
    }

    showMessage(text) {
        const content = this.container.querySelector('.screen-content');
        if (content) {
            content.innerHTML = `<div class="connecting">${text}</div>`;
        }
    }

    showStatusMessage(text, level = 'info') {
        // Status messages appear in the screen content via server updates
        console.log(`[${level}] ${text}`);
    }

    refreshEffect() {
        const screen = this.container.querySelector('.terminal-screen');
        if (screen) {
            screen.classList.add('refresh-flash');
            setTimeout(() => screen.classList.remove('refresh-flash'), 100);
        }
    }

    bell() {
        const screen = this.container.querySelector('.terminal-screen');
        if (screen) {
            screen.style.filter = 'brightness(1.5) invert(0.1)';
            setTimeout(() => {
                screen.style.filter = '';
            }, 100);
        }
    }

    showBusy() {
        this.busyStartTime = Date.now();
        const indicator = this.container.querySelector('.system-busy');
        if (indicator) {
            indicator.classList.add('active');
            this.updateBusyTimer();
            this.busyTimerInterval = setInterval(() => this.updateBusyTimer(), 100);
        }
    }

    hideBusy() {
        const indicator = this.container.querySelector('.system-busy');
        if (indicator) {
            indicator.classList.remove('active');
        }
        if (this.busyTimerInterval) {
            clearInterval(this.busyTimerInterval);
            this.busyTimerInterval = null;
        }
        this.busyStartTime = null;
    }

    updateBusyTimer() {
        if (!this.busyStartTime) return;
        const elapsed = (Date.now() - this.busyStartTime) / 1000;
        const timerEl = this.container.querySelector('.system-busy .timer');
        if (timerEl) {
            timerEl.textContent = elapsed.toFixed(1) + 's';
        }
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    render() {
        this.container.innerHTML = `
            <div class="terminal-screen">
                <div class="screen-content">
                    <div class="connecting">Initializing DK/400...</div>
                </div>
            </div>
            <div class="system-busy">
                <span class="indicator">X</span>
                <span class="timer">0.0s</span>
            </div>
            <div class="scanlines"></div>
            <div class="screen-flicker"></div>
            <div class="vignette"></div>
        `;
    }
}

// Initialize terminal when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.terminal = new Terminal5250('terminal');
});
