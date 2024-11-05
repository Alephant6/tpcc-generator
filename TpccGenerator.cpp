#include "TpccGenerator.hpp"
#include "CsvWriter.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cassert>
#include <cstring>

using namespace std;

enum TableID {
  WAREHOUSE = 0,
  DISTRICT,
  CUSTOMER,
  NEWORDER,
  ORDER,
  ITEM,
  STOCK,
  ORDERLINE,
  HISTORY,
  ORDERSTATUS
};

// Union for encoding TPCC keys
union TPCCKey {
  int64_t db_key;
  uint64_t alt_db_key;

  struct {
    uint64_t tabid : 4;
    uint64_t payload : 60;
  } tab;

  struct {
    uint64_t tabid : 4;
    uint64_t w_id : 14;
  } warehouse;

  struct {
    uint64_t tabid : 4;
    uint64_t w_id : 14;
    uint64_t d_id : 4;
  } district;

  struct {
    uint64_t tabid : 4;
    uint64_t w_id : 14;
    uint64_t d_id : 4;
    uint64_t c_id : 12;
  } customer;

  struct {
    uint64_t tabid : 4;
    uint64_t w_id : 14;
    uint64_t d_id : 4;
    uint64_t o_id : 24;
  } neworder, order;

  struct {
    uint64_t tabid : 4;
    uint64_t w_id : 14;
    uint64_t d_id : 4;
    uint64_t o_id : 24;
    uint64_t ol_number : 4;
  } orderline;

  struct {
    uint64_t tabid : 4;
    uint64_t i_id : 17;
  } item;

  struct {
    uint64_t tabid : 4;
    uint64_t w_id : 14;
    uint64_t i_id : 17;
  } stock;

  struct {
    uint64_t tabid : 4;
    uint64_t client_id : 14;
    uint64_t h_id : 40;
  } history;

  struct {
    uint64_t tabid : 4;
    uint64_t w_id : 14;
    uint64_t d_id : 4;
    uint64_t c_id : 12;
    uint64_t o_id : 24;
  } orderstatus;
};

void
EncodeDistrictKey(int64_t *db_key, uint64_t w_id, uint64_t d_id) {
  TPCCKey *key = (TPCCKey *)db_key;
  key->district.tabid = DISTRICT;
  key->district.w_id = w_id;
  key->district.d_id = d_id;
}

void
EncodeCustomerKey(int64_t *db_key, uint64_t w_id, uint64_t d_id,
                  uint64_t c_id) {
  TPCCKey *key = (TPCCKey *)db_key;
  key->customer.tabid = CUSTOMER;
  key->customer.w_id = w_id;
  key->customer.d_id = d_id;
  key->customer.c_id = c_id;
}

void
EncodeItemKey(int64_t *db_key, uint64_t i_id) {
  TPCCKey *key = (TPCCKey *)db_key;
  key->item.tabid = ITEM;
  key->item.i_id = i_id;
}

void
EncodeStockKey(int64_t *db_key, uint64_t w_id, uint64_t i_id) {
  TPCCKey *key = (TPCCKey *)db_key;
  key->stock.tabid = STOCK;
  key->stock.w_id = w_id;
  key->stock.i_id = i_id;
}

void
EncodeDistrictKey(int64_t *db_key, int64_t w_id, int64_t d_id) {
  TPCCKey *key = (TPCCKey *)db_key;
  key->district.tabid = DISTRICT;
  key->district.w_id = w_id;
  key->district.d_id = d_id;
}

void
EncodeCustomerKey(int64_t *db_key, int64_t w_id, int64_t d_id, int64_t c_id) {
  TPCCKey *key = (TPCCKey *)db_key;
  key->customer.tabid = CUSTOMER;
  key->customer.w_id = w_id;
  key->customer.d_id = d_id;
  key->customer.c_id = c_id;
}

void
EncodeItemKey(int64_t *db_key, int64_t i_id) {
  TPCCKey *key = (TPCCKey *)db_key;
  key->item.tabid = ITEM;
  key->item.i_id = i_id;
}

void
EncodeStockKey(int64_t *db_key, int64_t w_id, int64_t i_id) {
  TPCCKey *key = (TPCCKey *)db_key;
  key->stock.tabid = STOCK;
  key->stock.w_id = w_id;
  key->stock.i_id = i_id;
}

void
EncodeNewOrderKey(int64_t *db_key, int64_t w_id, int64_t d_id, int64_t o_id) {
  TPCCKey *key = (TPCCKey *)db_key;
  key->neworder.tabid = NEWORDER;
  key->neworder.w_id = w_id;
  key->neworder.d_id = d_id;
  key->neworder.o_id = o_id;
}

void
EncodeOrderKey(int64_t *db_key, int64_t w_id, int64_t d_id, int64_t o_id) {
  TPCCKey *key = (TPCCKey *)db_key;
  key->order.tabid = ORDER;
  key->order.w_id = w_id;
  key->order.d_id = d_id;
  key->order.o_id = o_id;
}

void
EncodeOrderLineKey(int64_t *db_key, int64_t w_id, int64_t d_id, int64_t o_id,
                   int64_t ol_number) {
  TPCCKey *key = (TPCCKey *)db_key;
  key->orderline.tabid = ORDERLINE;
  key->orderline.w_id = w_id;
  key->orderline.d_id = d_id;
  key->orderline.o_id = o_id;
  key->orderline.ol_number = ol_number;
}

void
EncodeOrderStatusKey(int64_t *db_key, int64_t w_id, int64_t d_id, int64_t c_id,
                     int64_t o_id) {
  TPCCKey *key = (TPCCKey *)db_key;
  key->orderstatus.tabid = ORDERSTATUS;
  key->orderstatus.w_id = w_id;
  key->orderstatus.d_id = d_id;
  key->orderstatus.c_id = c_id;
  key->orderstatus.o_id = o_id;
}

TpccGenerator::TpccGenerator(int64_t warehouse_count, const string &folder)
    : warehouse_count(warehouse_count), folder(folder), ranny(42) {}

void
TpccGenerator::generateWarehouses() {
  cout << "Generating warehouse .. " << flush;

  int64_t w_id;
  array<char, 10> w_name = {};
  array<char, 20> w_street_1 = {};
  array<char, 20> w_street_2 = {};
  array<char, 20> w_city = {};
  array<char, 2> w_state = {};
  array<char, 9> w_zip = {};
  float w_tax;
  float w_ytd;

  csv::CsvWriter w_csv(folder + "/warehouse.csv");

  for (w_id = 1L; w_id <= warehouse_count; w_id++) {
    makeAlphaString(6, 10, w_name.data());
    makeAddress(w_street_1.data(), w_street_2.data(), w_city.data(),
                w_state.data(), w_zip.data());
    w_tax = ((float)makeNumber(10L, 20L)) / 100.0f;
    w_ytd = 3000000.00f;

    // @formatter:off
    w_csv << w_id << w_name << w_street_1 << w_street_2 << w_city << w_state
          << w_zip << csv::Precision(4) << w_tax << csv::Precision(2) << w_ytd
          << csv::endl;
    // @formatter:on
  }

  cout << "ok !" << endl;
}

void
TpccGenerator::generateDistricts() {
  cout << "Generating districts .. " << flush;

  int64_t db_key;
  int64_t d_id;
  int64_t d_w_id;
  int64_t d_next_o_id;

  csv::CsvWriter d_csv(folder + "/district.csv");

  // Each warehouse has DIST_PER_WARE (10) districts
  for (d_w_id = 1; d_w_id <= warehouse_count; d_w_id++) {
    for (d_id = 1; d_id <= kDistrictsPerWarehouse; d_id++) {
      EncodeDistrictKey(&db_key, d_w_id, d_id);
      d_next_o_id = 3001;

      // @formatter:off
      d_csv << db_key <<  (int64_t)DISTRICT << csv::endl;
      // @formatter:on
    }
  }

  cout << "ok !" << endl;
}

void
TpccGenerator::generateCustomerAndHistory() {
  cout << "Generating customers and their history .. " << flush;

  int64_t db_key;
  int64_t c_id;
  int64_t c_d_id;
  int64_t c_w_id;
  int64_t c_balance;
  array<char, 500> c_data = {};
  float h_amount;
  array<char, 24> h_data = {};

  csv::CsvWriter c_csv(folder + "/customer.csv");

  // Each warehouse has DIST_PER_WARE (10) districts
  for (c_w_id = 1; c_w_id <= warehouse_count; c_w_id++) {
    for (c_d_id = 1; c_d_id <= kDistrictsPerWarehouse; c_d_id++) {
      for (c_id = 1; c_id <= kCustomerPerDistrict; c_id++) {
        EncodeCustomerKey(&db_key, c_w_id, c_d_id, c_id);
        c_balance = -1000;

        // @formatter:off
        c_csv << db_key <<  (int64_t)CUSTOMER << csv::endl;
        // @formatter:on
      }
    }
  }

  cout << "ok !" << endl;
}

void
TpccGenerator::generateItems() {
  cout << "Generating items .. " << flush;

  int64_t db_key;
  int64_t i_id;
  int64_t i_price;

  csv::CsvWriter i_csv(folder + "/item.csv");

  for (i_id = 1; i_id <= kItemCount; i_id++) {
    EncodeItemKey(&db_key, i_id);
    i_price = makeNumber(100L, 10000L);

    // @formatter:off
    i_csv << db_key << (int64_t) ITEM << csv::endl;
    // @formatter:on
  }

  cout << "ok !" << endl;
}

void
TpccGenerator::generateStock() {
  cout << "Generating stocks .. " << flush;

  int64_t db_key;
  int64_t s_i_id;
  int64_t s_w_id;
  int64_t s_quantity;
  int64_t s_ytd = 0;
  int64_t s_order_cnt = 0;
  int64_t s_remote_cnt = 0;

  csv::CsvWriter s_csv(folder + "/stock.csv");

  for (s_w_id = 1; s_w_id <= warehouse_count; s_w_id++) {
    for (s_i_id = 1; s_i_id <= kItemCount; s_i_id++) {
      EncodeStockKey(&db_key, s_w_id, s_i_id);
      s_quantity = makeNumber(10L, 100L);

      // @formatter:off
      s_csv << db_key << (int64_t) STOCK << csv::endl;
      // @formatter:on
    }
  }

  cout << "ok !" << endl;
}

void
TpccGenerator::generateOrdersAndOrderLines() {
  cout << "Generating orders .. " << flush;

  int64_t o_db_key;
  int64_t os_db_key;
  int64_t ol_db_key;
  int64_t no_db_key;
  int64_t o_id;
  int64_t o_c_id;
  int64_t o_d_id;
  int64_t o_w_id;
  int64_t o_carrier_id;
  int64_t o_ol_cnt;
  int64_t o_entry_d = 0;
  int64_t o_all_local = 1;

  int64_t ol_number;
  int64_t ol_i_id;
  int64_t ol_amount;
  array<char, 24> ol_dist_info = {};
  string kNull = "null";

  csv::CsvWriter o_csv(folder + "/order.csv");
  csv::CsvWriter os_csv(folder + "/order_status.csv");
  csv::CsvWriter ol_csv(folder + "/order_line.csv");
  csv::CsvWriter no_csv(folder + "/new_order.csv");

  // Generate ORD_PER_DIST (3000) orders and order line items for each district
  for (o_w_id = 1L; o_w_id <= warehouse_count; o_w_id++) {
    for (o_d_id = 1L; o_d_id <= kDistrictsPerWarehouse; o_d_id++) {
      // Each customer has exactly one order
      vector<uint32_t> customer_id_permutation =
          makePermutation(1, kCustomerPerDistrict + 1);

      for (o_id = 1; o_id <= OrdersPerDistrict; o_id++) {
        EncodeOrderKey(&o_db_key, o_w_id, o_d_id, o_id);
        o_c_id = customer_id_permutation[o_id - 1];
        EncodeOrderStatusKey(&os_db_key, o_w_id, o_d_id, o_c_id, o_id);
        o_carrier_id = makeNumber(1L, 10L);
        o_ol_cnt = makeNumber(5L, 15L);
        o_entry_d++;

        // @formatter:off
        o_csv << o_db_key <<  (int64_t)ORDER << csv::endl;
        // @formatter:on
        // @formatter:off
        os_csv << os_db_key << (int64_t)ORDERSTATUS << csv::endl;
        // @formatter:on

        // Order line items
        for (ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
          EncodeOrderLineKey(&ol_db_key, o_w_id, o_d_id, o_id, ol_number);
          ol_i_id = makeNumber(1L, kItemCount);

          if (o_id > 2100) {
            ol_amount = makeNumber(10L, 10000L);
            // @formatter:off
            ol_csv << ol_db_key << (int64_t) ORDERLINE
                   << csv::endl;
            // @formatter:on
          } else {
            ol_amount = 0L;
            // @formatter:off
            ol_csv << ol_db_key <<  (int64_t)ORDERLINE
                   << csv::endl;
            // @formatter:on
          }
        }

        // Generate a new order entry for the order for the last 900 rows
        if (o_id > 2100) {
          EncodeNewOrderKey(&no_db_key, o_w_id, o_d_id, o_id);
          no_csv << no_db_key << (int64_t) NEWORDER << csv::endl;
        }
      }
    }
  }

  cout << "ok !" << endl;
}

uint32_t
TpccGenerator::makeAlphaString(uint32_t min, uint32_t max, char *dest) {
  const static char *possible_values =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  uint32_t len = makeNumber(min, max);
  for (uint32_t i = 0; i < len; i++) {
    dest[i] = possible_values[ranny() % 62];
  }
  if (len < max) {
    dest[len] = '\0';
  }
  return len;
}

uint32_t
TpccGenerator::makeNumberString(uint32_t min, uint32_t max, char *dest) {
  const static char *possible_values = "0123456789";

  uint32_t len = makeNumber(min, max);
  for (uint32_t i = 0; i < len; i++) {
    dest[i] = possible_values[ranny() % 10];
  }
  if (len < max) {
    dest[len] = '\0';
  }
  return len;
}

void
TpccGenerator::makeAddress(char *street1, char *street2, char *city,
                           char *state, char *zip) {
  makeAlphaString(10, 20, street1);
  makeAlphaString(10, 20, street2);
  makeAlphaString(10, 20, city);
  makeAlphaString(2, 2, state);
  makeNumberString(9, 9, zip);  // XXX
}

uint32_t
TpccGenerator::makeNumber(uint32_t min, uint32_t max) {
  return ranny() % (max - min + 1) + min;
}

uint32_t
TpccGenerator::makeNonUniformRandom(uint32_t A, uint32_t x, uint32_t y) {
  return ((makeNumber(0, A) | makeNumber(x, y)) + 42) % (y - x + 1) + x;  // XXX
}

vector<uint32_t>
TpccGenerator::makePermutation(uint32_t min, uint32_t max) {
  assert(max > min);
  const uint32_t count = max - min;
  vector<uint32_t> result(count);
  iota(result.begin(), result.end(), min);

  for (uint32_t i = 0; i < count; i++) {
    swap(result[i], result[ranny() % count]);
  }
  return result;
}

void
TpccGenerator::makeLastName(int64_t num, char *name) {
  static const char *n[] = {"BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                            "ESE", "ANTI",  "CALLY", "ATION", "EING"};
  strcpy(name, n[num / 100]);
  strcat(name, n[(num / 10) % 10]);
  strcat(name, n[num % 10]);
}

void
TpccGenerator::makeNow(char *str) {
  string s =
      to_string(chrono::duration_cast<chrono::milliseconds>(
                    chrono::high_resolution_clock::now().time_since_epoch())
                    .count());  // XXX
  strncpy(str, s.data(), s.size());
}
