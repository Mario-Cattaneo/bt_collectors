// market_ui_fixed.js
// Fixed and completed version of the code you pasted.
// Summary of changes are below (see chat message):

// ------------------- Globals -------------------
const numbers = ["MARKET_umaBond", "MARKET_umaReward", "MARKET_orderPriceMinTickSize", "MARKET_orderMinSize", 
	"MARKET_customLiveness", "MARKTE_competitive", "MARKET_rewardsMinSize", "MARKET_rewardsMaxSpread", "MARKET_spread", 
	"MARKET_bestBid", "MARKET_bestAsk", "EVENTS_books_count", "EVENTS_price_change_count", "EVENTS_last_trade_price_count", 
	"EVENTS_tick_size_change_count", "BOOK_asymmetry", "BOOK_depth", "BOOK_discrepency", "BOOK_spread"];     // numeric fields identifiers
const times = ["MARKET_startDate", "MARKET_endDate", "MARKET_createdAt", "MARKET_updatedAt",
	"MARKET_closedAt", "MARKET_deployingTimestamp", "MARKET_umaEndDate", "MARKET_acceptingOrdersTimestamp", "market_found"]; // time fields identifiers
const strings = ["MARKET_id", "MARKET_conditionId", "MARKET_question", "MARKET_questionID", "MARKET_negRiskMarketID",
	"MARKET_umaResolutionStatuses"];	// string fields identifiers
const predicates = ["MARKET_negRisk", "MARKET_enableOrderBook", "MARKET_acceptingOrders", "MARKET_holdingRewardsEnabled", "MARKET_feesEnabled"];       // predicate fields

// ------------------- Tokenizer -------------------
function tokenize(input) {
    const regex = /\s*([()!&|+\-\*]|>=|<=|!=|=|>|<|\d+(?:\.\d+)?|"(?:\\"|[^"])*"|\w+)\s*/g;
    let tokens = [];
    let match;
    while ((match = regex.exec(input)) !== null) {
        tokens.push(match[1]);
    }
    return tokens;
}

// ------------------- AST Node -------------------
class ASTNode {
    constructor(type, value = null, children = []) {
        this.type = type;       // "number", "time", "string", "predicate", "operator", "identifier"
        this.value = value;
        this.children = children;
    }
}

// ------------------- Parser -------------------
function parseExpression(tokens) {
    let pos = 0;
    function peek() { return tokens[pos]; }
    function consume() { return tokens[pos++]; }

    function factor() {
        let tok = peek();
        if (!tok) throw new Error('Unexpected end of input');
        if (tok === '(') {
            consume();
            const node = expr();
            if (consume() !== ')') throw new Error("Missing closing parenthesis");
            return node;
        }
        if (/^\d+(?:\.\d+)?$/.test(tok)) {
            consume();
            return new ASTNode('number', parseFloat(tok));
        }
        if (/^"(?:\\"|[^"])*"$/.test(tok)) {
            consume();
            return new ASTNode('string', tok.slice(1, -1));
        }
        // identifier or predicate
        consume();
        return new ASTNode('identifier', tok);
    }

    // term: factor (( '*' | '+' | '-' ) factor)*
    function term() {
        let node = factor();
        while (peek() && ['*','+','-'].includes(peek())) {
            let op = consume();
            node = new ASTNode('operator', op, [node, factor()]);
        }
        return node;
    }

    // comparison: term ( (=|!=|>|<|>=|<=) term )*
    function comparison() {
        let node = term();
        while (peek() && ['=','!=','>','<','>=','<='].includes(peek())) {
            let op = consume();
            node = new ASTNode('operator', op, [node, term()]);
        }
        return node;
    }

    // logical: comparison ( (&|\|) comparison )* with unary '!'
    function logical() {
        // handle unary '!'
        if (peek() === '!') {
            consume();
            const child = logical();
            return new ASTNode('operator', '!', [child]);
        }
        let node = comparison();
        while (peek() && ['&','|'].includes(peek())) {
            let op = consume();
            node = new ASTNode('operator', op, [node, comparison()]);
        }
        return node;
    }

    function expr() { return logical(); }

    const tree = expr();
    if (pos !== tokens.length) throw new Error("Unexpected token: " + peek());
    return tree;
}

// ------------------- AST Evaluator / Type Inference -------------------
// returns one of: 'number', 'string', 'time', 'predicate', or null (invalid)
function inferType(node) {
    if (!node) return null;
    if (node.type === 'number') return 'number';
    if (node.type === 'string') return 'string';
    if (node.type === 'identifier') {
        if (numbers.includes(node.value)) return 'number';
        if (times.includes(node.value)) return 'time';
        if (strings.includes(node.value)) return 'string';
        if (predicates.includes(node.value)) return 'predicate';
        // unknown identifier
        return null;
    }
    if (node.type === 'operator') {
        const op = node.value;
        if (['+','-','*'].includes(op)) {
            // arithmetic: both children must be numeric (or time treated as numeric if you allow)
            const a = inferType(node.children[0]);
            const b = inferType(node.children[1]);
            if (a === 'number' && b === 'number') return 'number';
            return null;
        }
        if (['=','!=','>','<','>=','<='].includes(op)) {
            // comparisons return predicate if operands are comparable (numbers, times, strings)
            const a = inferType(node.children[0]);
            const b = inferType(node.children[1]);
            if (a && b && (a === b || ( (a==='number'||a==='time') && (b==='number'||b==='time')) )) return 'predicate';
            return null;
        }
        if (op === '!') {
            const c = inferType(node.children[0]);
            return (c === 'predicate') ? 'predicate' : null;
        }
        if (op === '&' || op === '|') {
            const a = inferType(node.children[0]);
            const b = inferType(node.children[1]);
            if (a === 'predicate' && b === 'predicate') return 'predicate';
            return null;
        }
    }
    return null;
}

// ------------------- Validator -------------------
function validateAST(node) {
    return inferType(node) !== null;
}

// ------------------- Helpers -------------------
function is_number(str) {
    try {
        const tokens = tokenize(str);
        const tree = parseExpression(tokens);
        const t = inferType(tree);
        return validateAST(tree) && t === 'number';
    } catch(e) {
        return false;
    }
}

function is_predicate(str) {
    try {
        const tokens = tokenize(str);
        const tree = parseExpression(tokens);
        const t = inferType(tree);
        return validateAST(tree) && t === 'predicate';
    } catch(e) {
        return false;
    }
}

function is_time(str) {
    // Accept numeric timestamps or ISO strings or identifiers from 'times'
    try {
        if (/^\d+$/.test(str)) return true;
        const date = new Date(str);
        if (!isNaN(date.getTime())) return true;
        // maybe it's an identifier
        return times.includes(str);
    } catch {
        return false;
    }
}

// ------------------- Market / Token classes (kept as-is) -------------------
class market_information { /* ... kept simple for demo */ }
class token { /* ... kept simple for demo */ };

// ------------------- UI state -------------------
const tokens_per_page = 15;
let token_index = 1;
let token_count = 15;
let token_rows = [];
let refresh_rate = 0.2;
let last_refresh = "-";

// DOM tree init
const body = document.body;

// Generic pop up
const popup = document.createElement('div');
const popup_overlay = document.createElement('div');
const popup_top_bar = document.createElement('div');
const close_popup = document.createElement('div');
const popup_content = document.createElement('div');
function dom_popup_init() {
    popup.classList.add('popup-panel');
    popup_overlay.classList.add('popup-overlay');
    popup_top_bar.classList.add('popup-top-bar');
    close_popup.textContent = '✖';
    close_popup.classList.add('popup-close');
    close_popup.addEventListener('click', () => popup_overlay.remove());
    popup_top_bar.append(close_popup);
    popup.append(popup_top_bar);
    popup_content.classList.add('popup-content');
    popup.append(popup_content);
    popup_overlay.appendChild(popup);
}

function clear_pop_up_tabs() {
    while (popup_top_bar.children.length > 1) {
        popup_top_bar.removeChild(popup_top_bar.lastChild);
    }
}

// Top bar
const top_bar = document.createElement('div');
const refresh_rate_view = document.createElement('div');
const last_refresh_view = document.createElement('div');
const query_selector = document.createElement('div');
function dom_top_bar_init() {
    top_bar.id = 'top-bar';
    top_bar.classList.add('bar');
    body.appendChild(top_bar);
    refresh_rate_view.id = 'refresh-rate';
    refresh_rate_view.textContent = "Refresh rate: " + refresh_rate + 's';
    last_refresh_view.id = 'last-refresh';
    last_refresh_view.textContent = "Last refresh: " + last_refresh;

    query_selector.id = 'query-selector';
    query_selector.textContent = '\u25BE'; // ▼
    query_selector.addEventListener('click', () => {render_pop_up(); render_query_tab();});

    top_bar.append(refresh_rate_view, last_refresh_view, query_selector);
}

// Token rows
const rows_container = document.createElement('div');
function dom_rows_init() {
    rows_container.id = 'rows-container';
    body.appendChild(rows_container);

    for (let token_row = 0; token_row < tokens_per_page; token_row++) {
        token_rows[token_row] = document.createElement('div');
        token_rows[token_row].classList.add('token-row');
        token_rows[token_row].textContent = "Token Row " + (token_row + 1);
        rows_container.appendChild(token_rows[token_row]);
    }
}

// Bottom bar
const bottom_bar = document.createElement('div');
const search_by_index = document.createElement('div');
const index_search_bar = document.createElement('input');
const index_search_submit = document.createElement('button');
const index_overview = document.createElement('div');
const change_page = document.createElement('div');
const prev_page = document.createElement('div');
const next_page = document.createElement('div');

function dom_bottom_bar_init() {
    bottom_bar.id = 'bottom-bar';
    bottom_bar.classList.add('bar');
    body.appendChild(bottom_bar);
    search_by_index.id = 'search-by-index';
    index_search_bar.type = 'text';
    index_search_bar.placeholder = 'index';
    index_search_bar.id = 'index-search';
    index_search_submit.id = 'index-submit';
    index_search_submit.textContent = 'Search';
    search_by_index.append(index_search_bar, index_search_submit);
    index_overview.id = 'index-overview';
    index_overview.textContent = "1 ... " + tokens_per_page;
    change_page.id = 'change-page';
    prev_page.id = 'prev-page';
    prev_page.textContent = '<';
    next_page.id = 'next-page';
    next_page.textContent = '>';
    change_page.append(prev_page, next_page);
    bottom_bar.append(search_by_index, index_overview, change_page);

    prev_page.addEventListener('click', () => { /* page logic placeholder */ });
    next_page.addEventListener('click', () => { /* page logic placeholder */ });

    index_search_submit.addEventListener('click', () => {
        const v = index_search_bar.value.trim();
        if (/^\d+$/.test(v)) {
            const idx = parseInt(v, 10);
            if (idx >= 1 && idx <= token_count) {
                alert('Would jump to index: ' + idx);
            } else {
                alert('Index out of range');
            }
        } else alert('Please enter a numeric index');
    });
}

// query tabs
let active_query_tab = 0; // 0 = order, 1 = filter, 2 = docs

// ----------- DOM Elements -----------
// Order
const order_selector = document.createElement('div');
const order_tab = document.createElement('div');
const order_input = document.createElement('input');
const apply_order = document.createElement('div');
let order_direction = 0; // 0 = desc, 1 = asc
const order_asc_opt = document.createElement('div');
const order_desc_opt = document.createElement('div');
let curr_order = document.createElement('div')
const order_docs = document.createElement('div');

// Filter
const filter_selector = document.createElement('div');
const filter_tab = document.createElement('div');
const filter_input = document.createElement('input');
const apply_filter = document.createElement('div');
const curr_filter = document.createElement('div');
const filter_docs = document.createElement('div');

// Docs
const query_docs_selector = document.createElement('div');
const query_docs_tab = document.createElement('div');

// internal state for applied values
let curr_order_str = '';
let curr_filter_str = '';

// ----------- Render Tabs -----------
function render_query_tab() {
  popup_content.innerHTML = '';
  clear_pop_up_tabs();
  popup_content.append(order_selector, filter_selector, query_docs_selector);

  if (active_query_tab === 0) {
    popup_content.append(order_tab);
  } else if (active_query_tab === 1) {
    popup_content.append(filter_tab);
  } else {
    popup_content.append(query_docs_tab);
  }
}

// ----------- Init Popup -----------
function dom_query_tabs_init() {
	curr_filter.textContent = 'None';
	curr_filter.classList.add('curr-filter-display');

	curr_order.textContent = 'None';
	curr_order.classList.add('curr-order-display');

	// selectors
	order_selector.classList.add('query-selector');
	order_selector.textContent = "Order";
	order_selector.addEventListener('click', () => {
		active_query_tab = 0;
		render_query_tab();
	});

	filter_selector.classList.add('query-selector');
	filter_selector.textContent = "Filter";
	filter_selector.addEventListener('click', () => {
		active_query_tab = 1;
		render_query_tab();
	});

	query_docs_selector.classList.add('query-selector');
	query_docs_selector.textContent = "Docs";
	query_docs_selector.addEventListener('click', () => {
		active_query_tab = 2;
		render_query_tab();
	});

	// order tab contents
	order_tab.classList.add('query-tab');
	order_input.classList.add('query-input');
	order_input.placeholder = "Enter order expression";
	apply_order.classList.add('query-apply');
	apply_order.textContent = "Apply Order";
	order_asc_opt.classList.add('order-opt');
	order_asc_opt.textContent = "Asc";
	order_desc_opt.classList.add('order-opt');
	order_desc_opt.textContent = "Desc";

	// toggle asc/desc
	order_asc_opt.addEventListener('click', () => {
		order_direction = 1;
		order_asc_opt.classList.add('active');
		order_desc_opt.classList.remove('active');
	});
	order_desc_opt.addEventListener('click', () => {
		order_direction = 0;
		order_desc_opt.classList.add('active');
		order_asc_opt.classList.remove('active');
	});

	// show available options
	order_docs.classList.add('query-docs');
	order_docs.innerHTML = `
		<strong>Order Docs:</strong><br>
		Valid numbers: ${numbers.join(", ") || "none"}<br>
		Valid times: ${times.join(", ") || "none"}<br>
		Operators: +, -, *, =, >, <, >=, <=, !=<br>
		Associativity: left-to-right<br>
	`;

	order_tab.append(order_input, apply_order, order_asc_opt, order_desc_opt, curr_order, order_docs);

	// filter tab
	filter_tab.classList.add('query-tab');
	filter_input.classList.add('query-input');
	filter_input.placeholder = "Enter filter expression";
	apply_filter.classList.add('query-apply');
	apply_filter.textContent = "Apply Filter";
	filter_docs.classList.add('query-docs');
	filter_docs.innerHTML = `
		<strong>Filter Docs:</strong><br>
		Valid strings: ${strings.join(", ") || "none"}<br>
		Valid predicates: ${predicates.join(", ") || "none"}<br>
		Operators: =, !=, &, |, !<br>
	`;
	filter_tab.append(filter_input, apply_filter, curr_filter, filter_docs);

	// docs tab
	query_docs_tab.classList.add('query-tab');
	query_docs_tab.innerHTML = `
		<strong>General Docs:</strong><br>
		- Numbers: composed with +, -, *<br>
		- Times: can be timestamps or ISO strings<br>
		- Strings: can be compared with =<br>
		- Predicates: combined with &, |, !<br>
		<br>
		Precedence: *,-,+, =, >, <, >=, <=, !=, !, &, |<br>
		Associativity: left to right
	`;

	// input handling
	order_input.addEventListener('input', (event) => {
		if (!(is_number(event.target.value) || is_time(event.target.value))) {
			order_input.style.backgroundColor = '#ffe5e5'; // light red
		} else {
			order_input.style.backgroundColor = 'white';
		}
	});

	filter_input.addEventListener('input', (event) => {
		if (!is_predicate(event.target.value)) {
			filter_input.style.backgroundColor = '#ffe5e5'; // light red
		} else {
			filter_input.style.backgroundColor = 'white';
		}
	});

	apply_order.addEventListener('click', () => {
		if (order_input.style.backgroundColor !== 'white') return;

		if (order_input.value.trim() === '') {
			curr_order.textContent = 'None';
		} else {
			curr_order.textContent = order_input.value.trim() + (order_direction === 1 ? ' (Asc)' : ' (Desc)');
		}
	});

	apply_filter.addEventListener('click', () => {
		if (filter_input.style.backgroundColor !== 'white') return;

		if (filter_input.value.trim() === '') {
			curr_filter.textContent = 'None';
		} else {
			curr_filter.textContent = filter_input.value.trim();
		}
	});
}

// ----------- Open Popup -----------
function render_pop_up() {
    document.body.appendChild(popup_overlay);
    render_query_tab();
}

function dom_init() {
    dom_popup_init();
    dom_top_bar_init();
    dom_rows_init();
    dom_bottom_bar_init();
    dom_query_tabs_init();
}

dom_init();

//timestamp: new Date().toISOString()
// --- Async POST function ---
async function refresh() {
    try {
        // Choose your URL: either IP with http or hostname with https
        const url = "http://localhost:8080/refresh";

        const payload = {
            curr_order: curr_order.textContent,
            curr_filter: curr_filter.textContent
        };

        const response = await fetch(url, {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            throw new Error("HTTP error " + response.status);
        }

        const data = await response.json();
        console.log("POST success:", data);

        // update last refresh timestamp in UI
        last_refresh = new Date().toLocaleTimeString();
        last_refresh_view.textContent = "Last refresh: " + last_refresh;

    } catch (err) {
        console.error("POST failed:", err);
	}
}



// Expose some helpers for quick debugging in console
/*window._tokenize = tokenize;
window._parseExpression = function(s) { return parseExpression(tokenize(s)); };
window._inferType = inferType;
window._is_number = is_number;
window._is_predicate = is_predicate;
window._is_time = is_time;
*/
// End of file
