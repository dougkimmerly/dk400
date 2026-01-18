/**
 * DK/400 - IBM 5250 Terminal Emulator
 * WebSocket-based terminal client with authentic AS/400 feel
 */

class Terminal5250 {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.ws = null;
        this.screenData = [];
        this.cursorRow = 0;
        this.cursorCol = 0;
        this.currentScreen = 'signon';
        this.inputFields = [];
        this.activeFieldIndex = 0;
        this.connected = false;

        // Screen dimensions (80x24 main + status lines)
        this.cols = 80;
        this.rows = 24;

        // Keyboard sounds (optional)
        this.soundEnabled = false;

        this.init();
    }

    init() {
        this.render();
        this.connect();
        this.setupKeyboardHandler();
    }

    connect() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws`;

        this.showMessage('Connecting to DK/400...');

        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
            this.connected = true;
            this.showMessage('Connected');
            // Request initial screen
            this.send({ action: 'init' });
        };

        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.handleMessage(data);
        };

        this.ws.onclose = () => {
            this.connected = false;
            this.showMessage('Connection lost. Reconnecting...');
            setTimeout(() => this.connect(), 3000);
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
            this.showMessage('Connection error');
        };
    }

    send(data) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(data));
        }
    }

    handleMessage(data) {
        switch (data.type) {
            case 'screen':
                this.updateScreen(data);
                break;
            case 'cursor':
                this.moveCursor(data.row, data.col);
                break;
            case 'message':
                this.showStatusMessage(data.text, data.level);
                break;
            case 'clear':
                this.clearScreen();
                break;
            case 'refresh':
                this.refreshEffect();
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

        this.renderScreen();

        // Focus the active input field
        if (this.inputFields.length > 0) {
            this.focusField(this.activeFieldIndex);
        }

        // Add refresh flash effect
        this.refreshEffect();
    }

    renderScreen() {
        const content = this.container.querySelector('.screen-content');
        if (!content) return;

        let html = '';

        this.screenData.forEach((row, rowIndex) => {
            html += `<div class="screen-row" data-row="${rowIndex}">`;

            if (typeof row === 'string') {
                // Simple text row
                html += this.escapeHtml(row.padEnd(this.cols));
            } else if (Array.isArray(row)) {
                // Row with field segments
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
                        const password = segment.password ? 'type="password"' : 'type="text"';

                        html += `<input
                            id="${fieldId}"
                            class="input-field ${cssClass}"
                            ${password}
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

        // Re-attach event listeners to input fields
        this.attachFieldListeners();
    }

    attachFieldListeners() {
        const inputs = this.container.querySelectorAll('.input-field');

        inputs.forEach((input, index) => {
            input.addEventListener('keydown', (e) => this.handleFieldKeydown(e, index));
            input.addEventListener('input', (e) => this.handleFieldInput(e, index));
            input.addEventListener('focus', () => {
                this.activeFieldIndex = index;
            });
        });
    }

    handleFieldKeydown(e, fieldIndex) {
        const field = this.inputFields[fieldIndex];

        switch (e.key) {
            case 'Enter':
                e.preventDefault();
                this.submitScreen();
                break;

            case 'Tab':
                e.preventDefault();
                if (e.shiftKey) {
                    this.focusPreviousField();
                } else {
                    this.focusNextField();
                }
                break;

            case 'ArrowDown':
                e.preventDefault();
                this.focusNextField();
                break;

            case 'ArrowUp':
                e.preventDefault();
                this.focusPreviousField();
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
                this.handleFunctionKey('F12'); // ESC = Cancel
                break;

            case 'PageUp':
                e.preventDefault();
                this.handleFunctionKey('PageUp');
                break;

            case 'PageDown':
                e.preventDefault();
                this.handleFunctionKey('PageDown');
                break;
        }

        // Play keystroke sound
        if (this.soundEnabled) {
            this.playKeystroke();
        }
    }

    handleFieldInput(e, fieldIndex) {
        // Send field update to server
        const fieldId = this.inputFields[fieldIndex]?.id;
        if (fieldId) {
            this.send({
                action: 'field_update',
                field: fieldId,
                value: e.target.value
            });
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
        const nextIndex = (this.activeFieldIndex + 1) % inputs.length;
        this.focusField(nextIndex);
    }

    focusPreviousField() {
        const inputs = this.container.querySelectorAll('.input-field');
        const prevIndex = (this.activeFieldIndex - 1 + inputs.length) % inputs.length;
        this.focusField(prevIndex);
    }

    submitScreen() {
        // Gather all field values
        const fieldValues = {};
        const inputs = this.container.querySelectorAll('.input-field');

        inputs.forEach(input => {
            const fieldId = input.dataset.fieldId;
            fieldValues[fieldId] = input.value;
        });

        this.send({
            action: 'submit',
            screen: this.currentScreen,
            fields: fieldValues
        });
    }

    handleFunctionKey(key) {
        // Gather current field values
        const fieldValues = {};
        const inputs = this.container.querySelectorAll('.input-field');
        inputs.forEach(input => {
            const fieldId = input.dataset.fieldId;
            fieldValues[fieldId] = input.value;
        });

        this.send({
            action: 'function_key',
            key: key,
            screen: this.currentScreen,
            fields: fieldValues
        });
    }

    setupKeyboardHandler() {
        // Global keyboard handler for when no field is focused
        document.addEventListener('keydown', (e) => {
            // If an input field is focused, let it handle the event
            if (document.activeElement.classList.contains('input-field')) {
                return;
            }

            // Handle function keys globally
            if (e.key.startsWith('F') || e.key === 'Escape') {
                e.preventDefault();

                if (e.key === 'Escape') {
                    this.handleFunctionKey('F12');
                } else {
                    this.handleFunctionKey(e.key);
                }
            }

            // Any other key - focus first input field
            if (e.key.length === 1 && !e.ctrlKey && !e.altKey && !e.metaKey) {
                const firstInput = this.container.querySelector('.input-field');
                if (firstInput) {
                    firstInput.focus();
                }
            }
        });

        // Handle clicks on function key labels
        document.querySelectorAll('.fkey').forEach(fkey => {
            fkey.addEventListener('click', () => {
                const key = fkey.dataset.key;
                if (key) {
                    this.handleFunctionKey(key);
                }
            });
        });
    }

    clearScreen() {
        this.screenData = [];
        this.inputFields = [];
        this.renderScreen();
    }

    showMessage(text) {
        const content = this.container.querySelector('.screen-content');
        if (content) {
            content.innerHTML = `<div class="connecting">${text}</div>`;
        }
    }

    showStatusMessage(text, level = 'info') {
        const statusBar = this.container.querySelector('.message-area');
        if (statusBar) {
            const cssClass = level === 'error' ? 'field-error' :
                            level === 'warning' ? 'field-warning' : '';
            statusBar.innerHTML = `<span class="${cssClass}">${this.escapeHtml(text)}</span>`;
        }
    }

    refreshEffect() {
        const screen = this.container.querySelector('.terminal-screen');
        if (screen) {
            screen.classList.add('refresh-flash');
            setTimeout(() => screen.classList.remove('refresh-flash'), 100);
        }
    }

    bell() {
        // Visual bell - flash the screen
        const screen = this.container.querySelector('.terminal-screen');
        if (screen) {
            screen.style.filter = 'brightness(1.5) invert(0.1)';
            setTimeout(() => {
                screen.style.filter = '';
            }, 100);
        }

        // Audio bell (if enabled)
        if (this.soundEnabled) {
            this.playBell();
        }
    }

    playKeystroke() {
        // TODO: Add keystroke sound
    }

    playBell() {
        // TODO: Add bell sound
    }

    moveCursor(row, col) {
        this.cursorRow = row;
        this.cursorCol = col;
        // Cursor is handled by input field focus
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    render() {
        this.container.innerHTML = `
            <div class="crt-monitor">
                <div class="power-led"></div>
                <div class="monitor-badge">IBM</div>
                <div class="screen-container">
                    <div class="terminal-screen">
                        <div class="screen-content">
                            <div class="connecting">Initializing DK/400...</div>
                        </div>
                        <div class="scanlines"></div>
                        <div class="screen-flicker"></div>
                        <div class="vignette"></div>
                    </div>
                </div>
                <div class="function-keys">
                    <span class="fkey" data-key="F1">F1=Help</span>
                    <span class="fkey" data-key="F3">F3=Exit</span>
                    <span class="fkey" data-key="F4">F4=Prompt</span>
                    <span class="fkey" data-key="F5">F5=Refresh</span>
                    <span class="fkey" data-key="F6">F6=Create</span>
                    <span class="fkey" data-key="F9">F9=Retrieve</span>
                    <span class="fkey" data-key="F12">F12=Cancel</span>
                </div>
            </div>
        `;

        // Add message area to screen content will be done in renderScreen
    }

    // Toggle sound effects
    toggleSound() {
        this.soundEnabled = !this.soundEnabled;
        return this.soundEnabled;
    }
}

// Initialize terminal when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.terminal = new Terminal5250('terminal');
});
