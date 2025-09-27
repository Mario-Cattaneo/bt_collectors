/*
const orderable_attributes = {
	FOUND_TIME: 0,
  	MARKET_startDate,
	MARKET_endDate,
	MARKET_updatedAt,
	MARKET_createdAt,
	MARKET_closedAt,
	MARKET_deployingTimestamp,
	MARKET_umaEndDate,
	MARKET_umaBond, 
	MARKET_umaReward,
	MARKET_orderPriceMinTickSize,
	MARKET_orderMinSize,
	MARKET_customLiveness,
	MARKET_acceptingOrdersTimestamp,
	MARKET_competitive,
	MARKET_rewardsMinSize,
	MARKET_rewardsMaxSpread,
	MARKET_spread,
	MARKET_bestBid,
	MARKET_bestAsk,

};

const boolean_attributes = {

}

const enum_attributes = {

}

const event_metrics = {
	EVENTS_books_count,
	EVENTS_price_change_count,
	EVENTS_last_trade_price_count,
	EVENTS_tick_size_change_count,
	BOOK_ASYMMETRY: 5,
  	BOOK_DEPTH: 6,
  	BOOK_DISCREPENCY: 7,
  	BOOK_SPREAD: 8
}


const filter = {
	MARKET_id, // filter by list of strings
	MARKET_negRisk,
	MARKET_enableOrderBook,
	MARKET_umaResolutionStatuses, // string comparison
	MARKET_acceptingOrders,
	MARKET_holdingRewardsEnabled,
	MARKET_feesEnabled,


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
*/
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
const popup = document.createElement('div');
const popup_overlay = document.createElement('div');
const close_popup = document.createElement('div');
close_popup.textContent = '✖';
close_popup.classList.add('popup-close');
close_popup.addEventListener('click', () => popup_overlay.remove());
popup.append(close_popup);
popup_overlay.appendChild(popup);

// Top bar
const top_bar = document.createElement('div');
top_bar.id = 'top-bar';
top_bar.classList.add('bar');
body.appendChild(top_bar);

const refresh_rate_view = document.createElement('div');
refresh_rate_view.id = 'refresh-rate';
refresh_rate_view.textContent = "Refresh rate: " + refresh_rate;

const last_refresh_view = document.createElement('div');
last_refresh_view.id = 'last-refresh';
last_refresh_view.textContent = "Last refresh: " + last_refresh;

const query_selector = document.createElement('div');
query_selector.id = 'query-selector';
query_selector.textContent = '\u25BE'; // ▼
query_selector.addEventListener('click', on_click_query_selector);

top_bar.append(refresh_rate_view, last_refresh_view, query_selector);

// Scrollable rows container
const rows_container = document.createElement('div');
rows_container.id = 'rows-container';
body.appendChild(rows_container);

for (let token_row = 0; token_row < 15; token_row++) {
  token_rows[token_row] = document.createElement('div');
  token_rows[token_row].classList.add('token-row');
  token_rows[token_row].textContent = "Token Row " + (token_row + 1);
  rows_container.appendChild(token_rows[token_row]);
}

// Bottom bar
const bottom_bar = document.createElement('div');
bottom_bar.id = 'bottom-bar';
bottom_bar.classList.add('bar');
body.appendChild(bottom_bar);

const search_by_index = document.createElement('div');
search_by_index.id = 'search-by-index';

const index_search_bar = document.createElement('input');
index_search_bar.type = 'text';
index_search_bar.placeholder = 'index'; // ✅ hint text
index_search_bar.id = 'index-search';

const index_search_submit = document.createElement('button');
index_search_submit.id = 'index-submit';
index_search_submit.textContent = 'Search'; // ✅ button text

search_by_index.append(index_search_bar, index_search_submit);

const index_overview = document.createElement('div');
index_overview.id = 'index-overview';
index_overview.textContent = "1 ... 15";

const change_page = document.createElement('div');
change_page.id = 'change-page';

const prev_page = document.createElement('div');
prev_page.id = 'prev-page';
prev_page.textContent = '<';

const next_page = document.createElement('div');
next_page.id = 'next-page';
next_page.textContent = '>';

change_page.append(prev_page, next_page);

bottom_bar.append(search_by_index, index_overview, change_page);

function on_click_query_selector() {
  document.body.appendChild(popup_overlay);
}

function dom_init() {
	popup.classList.add('popup-panel');
	popup_overlay.classList.add('popup-overlay');
}
