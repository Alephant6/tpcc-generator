#include "TpccGenerator.hpp"

#include <iostream>
#include <fstream>
#include <filesystem>
#include <vector>
#include <string>

using namespace std;

namespace fs = std::filesystem;

void
combineFiles(const std::string& directoryPath,
             const std::string& outputFileName) {
  std::string outputFilePath = directoryPath + "/" + outputFileName;

  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(directoryPath)) {
    if (entry.is_regular_file() && entry.path().filename() != outputFileName) {
      files.push_back(entry.path());
    }
  }

  std::ofstream outputFile(outputFilePath, std::ios::trunc);
  if (!outputFile.is_open()) {
    std::cerr << "Failed to open output file." << std::endl;
    return;
  }

  for (const auto& filePath : files) {
    std::ifstream inputFile(filePath);
    if (inputFile.is_open()) {
      outputFile << inputFile.rdbuf();
      inputFile.close();
    } else {
      std::cerr << "Failed to open file: " << filePath << std::endl;
    }
  }
  outputFile.close();
}

int
main(int argc, char** argv) {
  // Check input
  if (argc != 3) {
    cout << "Usage: " << argv[0] << " <warehouse_count> <output_path>" << endl;
    return -1;
  }

  // Parse input
  char* end_ptr;
  long warehouse_count = strtol(argv[1], &end_ptr, 10);
  if (*end_ptr != '\0' || warehouse_count > (1ll << 32) - 1 ||
      warehouse_count < 0) {
    cout << "ERROR: I can not parse '" << argv[1]
         << "' as a 32 bit unsigned integer :(" << endl;
    cout << "Usage: " << argv[0] << " <warehouse_count> <output_path>" << endl;
    return -1;
  }

  // Generate tpcc data
  TpccGenerator generator((uint32_t)warehouse_count, argv[2]);

  cout << "I am loading TPCC data for " << warehouse_count << " warehouse"
       << (warehouse_count != 1 ? "s" : "") << ", hold on .." << endl
       << endl;
  //  generator.generateWarehouses();
  generator.generateDistricts();
  generator.generateCustomerAndHistory();
  generator.generateItems();
  generator.generateStock();
  //  generator.generateOrdersAndOrderLines();
  cout << endl << ".. data generation completed successfully :)" << endl;

  combineFiles(argv[2], "tpcc_table.csv");

  return 0;
}
