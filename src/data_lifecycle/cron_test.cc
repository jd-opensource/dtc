#include <iostream>
#include <chrono>
#include <functional>
#include "croncpp.h"

int main(){
    try{
        auto cron = cron::make_cron("0 */5 * * * ?");
        std::time_t now = std::time(0);
        std::time_t next = cron::cron_next(cron, now);
        std::cout << next << std::endl;
    }
    catch (cron::bad_cronexpr const & ex){
        std::cerr << ex.what() << '\n';
    }
    return 0;
}