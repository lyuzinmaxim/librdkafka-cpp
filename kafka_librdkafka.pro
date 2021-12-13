QT -= gui

CONFIG += c++11 console
CONFIG -= app_bundle

# You can make your code fail to compile if it uses deprecated APIs.
# In order to do so, uncomment the following line.
#DEFINES += QT_DISABLE_DEPRECATED_BEFORE=0x060000    # disables all the APIs deprecated before Qt 6.0.0

SOURCES += \
        main.cpp \
        wingetopt.c

INCLUDEPATH += C:/librdkafka/librdkafka.redist.1.7.0/build/native/include/librdkafka
INCLUDEPATH += C:/librdkafka-master/src-cpp

#INCLUDEPATH += C:/kcat-master/win32

LIBS += -L"C:/librdkafka/librdkafka.redist.1.7.0/build/native/lib/win/x64/win-x64-Release/v120" -llibrdkafkacpp
LIBS += -L"C:/librdkafka/librdkafka.redist.1.7.0/build/native/lib/win/x64/win-x64-Release/v120" -llibrdkafka

#LIBS += C:/librdkafka/librdkafka.redist.1.7.0/runtimes/win-x64/native/librdkafka.dll
#LIBS += C:/librdkafka/librdkafka.redist.1.7.0/runtimes/win-x64/native/librdkafkacpp
#LIBS += C:/librdkafka/librdkafka.redist.1.7.0/runtimes/win-x64/native/libzstd
#LIBS += C:/librdkafka/librdkafka.redist.1.7.0/runtimes/win-x64/native/msvcp120
#LIBS += C:/librdkafka/librdkafka.redist.1.7.0/runtimes/win-x64/native/msvcr120
#LIBS += C:/librdkafka/librdkafka.redist.1.7.0/runtimes/win-x64/native/zlib

#LIBS += -L"C:\librdkafka\librdkafka.redist.1.7.0\runtimes\win-x64\native" -llibrdkafka
#LIBS += -L"C:\librdkafka\librdkafka.redist.1.7.0\runtimes\win-x64\native" -llibrdkafkacpp
#LIBS += -L"C:\librdkafka\librdkafka.redist.1.7.0\runtimes\win-x64\native" -llibzstd
#LIBS += -L"C:\librdkafka\librdkafka.redist.1.7.0\runtimes\win-x64\native" -lmsvcp120
#LIBS += -L"C:\librdkafka\librdkafka.redist.1.7.0\runtimes\win-x64\native" -lmsvcr120
#LIBS += -L"C:\librdkafka\librdkafka.redist.1.7.0\runtimes\win-x64\native" -lzlib




# Default rules for deployment.
qnx: target.path = /tmp/$${TARGET}/bin
else: unix:!android: target.path = /opt/$${TARGET}/bin
!isEmpty(target.path): INSTALLS += target

