#include <iostream>
#include <string>
#include "Skip_Database.h"

int main() {
    Skip_Database<std::string, std::string> sk(32, 360, 100, 200, 100);
    sk.run();
}
