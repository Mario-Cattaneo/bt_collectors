// ------------------- Globals -------------------
const numbers = ["nr1","nr3","nr7"];     // e.g., [1, 2, 3.14]
const times = ["hm","time?"];       // e.g., ["2025-09-27T15:23", 1695804180]
const strings = ["yuh","yur"];     // e.g., ["hello", "world"]
const predicates = ["yes", "noooo"];  // pre-defined predicates

// ------------------- Tokenizer -------------------
function tokenize(input) {
    const regex = /\s*([()!&|+\-*]|>=|<=|!=|=|>|<|\d+(\.\d+)?|".*?"|\w+)\s*/g;
    let tokens = [];
    let match;
    while ((match = regex.exec(input)) !== null) {
        tokens.push(match[1]);
    }
    return tokens;
}

// ------------------- AST Nodeclose_popup -------------------
class ASTNode {
    constructor(type, value=null, children=[]) {
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

    // factor: number, string, identifier, or parentheses
    function factor() {
        let tok = peek();
        if (tok === '(') {
            consume();
            const node = expr();
            if (consume() !== ')') throw new Error("Missing closing parenthesis");
            return node;
        }
        if (/^\d+(\.\d+)?$/.test(tok)) {
            consume();
            return new ASTNode('number', parseFloat(tok));
        }
        if (/^".*"$/.test(tok)) {
            consume();
            return new ASTNode('string', tok.slice(1, -1));
        }
        consume();
        return new ASTNode('identifier', tok);
    }

    // term: factor [*,-,+ factor]...
    function term() {
        let node = factor();
        while (['*','+','-'].includes(peek())) {
            let op = consume();
            node = new ASTNode('operator', op, [node, factor()]);
        }
        return node;
    }

    // comparison: term [=, !=, >, <, >=, <= term]...
    function comparison() {
        let node = term();
        while (['=','!=','>','<','>=','<='].includes(peek())) {
            let op = consume();
            node = new ASTNode('operator', op, [node, term()]);
        }
        return node;
    }

    // logical: comparison [&|! comparison]...
    function logical() {
        let node = comparison();
        while (['&','|'].includes(peek()) || peek() === '!') {
            let op = consume();
            if (op === '!') {
                node = new ASTNode('operator', '!', [logical()]);
            } else {
                node = new ASTNode('operator', op, [node, comparison()]);
            }
        }
        return node;
    }

    function expr() { return logical(); }

    const tree = expr();
    if (pos !== tokens.length) throw new Error("Unexpected token: " + peek());
    return tree;
}

// ------------------- Validator -------------------
function validateAST(node) {
    if (!node) return false;
    switch(node.type) {
        case 'number': return true;
        case 'string': return true;
        case 'identifier':
            return numbers.includes(node.value) || times.includes(node.value) || strings.includes(node.value);
        case 'operator':
            if (['+', '-', '*', '=', '!=', '>', '<', '>=', '<=', '&', '|'].includes(node.value))
                return node.children.every(validateAST);
            if (node.value === '!') return node.children.every(validateAST);
    }
    return false;
}

// ------------------- Helpers -------------------
function is_number(str) {
    try {
        const tokens = tokenize(str);
        const tree = parseExpression(tokens);
        return validateAST(tree) && tree.type === 'number';
    } catch(e) {
        return false;
    }
}

function is_predicate(str) {
    try {
        const tokens = tokenize(str);
        const tree = parseExpression(tokens);
        return validateAST(tree);
    } catch(e) {
        return false;
    }
}

function is_time(str) {
    try {
        const ts = parseInt(str);
        if (!isNaN(ts)) return true;
        const date = new Date(str);
        return !isNaN(date.getTime());
    } catch {
        return false;
    }
}


class market_information {
	id; // string
	conditionId; // string

	question; // string
	questionID; // string

	negRisk; // bool
	negRiskMarketID; // string

	startDate; // string
	endDate; // string
	createdAt; // string
	updatedAt; // string
	closedAt; // string
	deployingTimestamp; // string

	umaEndDate; // string
	umaBond; // integer
	umaReward; // integer
	umaResolutionStatuses; // string 
	
	enableOrderBook; // bool
	orderPriceMinTickSize; // real
	orderMinSize; // real
	umaResolutionStatus; // string

	customLiveness; // integer
	acceptingOrders; // bool
	acceptingOrdersTimestamp; // string
	competitive; // real

	rewardsMinSize; // integer
	rewardsMaxSpread; // real
	holdingRewardsEnabled; // bool
	feesEnabled; // bool

	spread; // real
	bestBid; // real
	bestAsk; // real
}

class token {
	id; // string
	side; // "yes" or "no"
	index;
	dom_element;
	market_information; // market_information instance
	complement_token; // other side in same market, type token
	other_negrisk_tokens; // array of other tokens involved in negrisk (can be empty)
};

const tokens_per_page = 15;
let token_index = 1; //
let token_count = 15; 
let token_rows = []; // must be 10 of type token


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

	for (let token_row = 0; token_row < 15; token_row++) {
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
	index_overview.textContent = "1 ... 15";
	change_page.id = 'change-page';
	prev_page.id = 'prev-page';
	prev_page.textContent = '<';
	next_page.id = 'next-page';
	next_page.textContent = '>';
	change_page.append(prev_page, next_page);
	bottom_bar.append(search_by_index, index_overview, change_page);
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

// ----------- Render Tabs -----------
function render_query_tab() {
  // clear previous content (only clears content area — top bar & selector bar remain)
  popup_content.innerHTML = '';
  clear_pop_up_tabs()
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
	curr_order.textContent = 'None';


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

	order_tab.append(order_input, apply_order, order_asc_opt, order_desc_opt, order_docs);

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
	filter_tab.append(filter_input, apply_filter, filter_docs);

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
	order_input.addEventListener('input', (input) => {
		if(!(is_number(input) || is_time(input))) {
			//make order input background slightly red
		}
	});

	filter_input.addEventListener('input', (input) => {
		if(!is_predicate(input)) {
			//make filter input background slightly red
		}
	});

	apply_order.addEventListener('click', () => {
		if(//order input colo not white) {
			return;
		}
		if(filter_input_str === '') {
			curr_order.textContent = 'None';

		} else {
			curr_order.textContent = // order_input content
		}
		return;
	});

	apply_filter.addEventListener('click', () => {
		//analogous to above
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
