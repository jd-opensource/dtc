#ifndef __H_WATCHDOG_HWC_H__
#define __H_WATCHDOG_HWC_H__

#include "base.h"

class WatchDogHWC : public WatchDogDaemon
{
public:
    WatchDogHWC(WatchDog *o, int sec);
    virtual~WatchDogHWC();
    virtual void exec(void);
};

#endif