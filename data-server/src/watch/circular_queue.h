_Pragma("once");

#include <iostream>
#include <string.h>
#include <vector>

namespace sharkstore {
namespace dataserver {
namespace watch {


/** * 环形队列类模板 * */
    template<class T>
    class CircularQueue {
    public:
        CircularQueue(uint32_t capacity);

        virtual ~CircularQueue();

        //清队
        void clearQueue();

        //入队
        bool enQueue(T element);

        //出队
        bool deQueue(T &element);

        int32_t getData(int64_t version, std::vector<T> &element);
        int64_t getUsedTime() {
            if(isEmpty()) {
                return 0;
            }

            return m_pQueue[m_iHead].usedTime();
        };

        //传入一个T的引用，方便接队头，而不是返回队头，这样函数返回布尔，调用完毕后，引用拿到队头
        //判空
        bool isEmpty() const;

        //判满
        bool isFull() const;

        //队长
        int length() const;

        //列队
        void printQueue(void(*pFunc)(T));
        //适配所有模板类的打印，传入一个对应类型的打印函数指针

    private:
        //队列数组指针
        T *m_pQueue = nullptr;
        //队列容量
        int32_t m_iCapacity{0};
        //队头
        int32_t m_iHead{0};
        //队尾
        int32_t m_iTail{0};
        //队长
        int32_t m_iLength{0};

    };

    template<class T>
    CircularQueue<T>::CircularQueue(uint32_t capacity) {
        m_iCapacity = capacity;
        clearQueue();
        m_pQueue = new T[m_iCapacity];
    }

    template<class T>
    CircularQueue<T>::~CircularQueue() {
        delete[]m_pQueue;
        m_pQueue = nullptr;
    }

    template<class T>
    void CircularQueue<T>::clearQueue() {
        m_iHead = 0;
        m_iTail = 0;
        m_iLength = 0;
    }

    template<class T>
    bool CircularQueue<T>::enQueue(T element) {
        if (isFull()) {
            T tmpElement;

            if(!deQueue(tmpElement)){
                return false;
            }
        }

        m_pQueue[m_iTail] = element;
        m_iLength++;
        m_iTail++;
        m_iTail = m_iTail % m_iCapacity;
        return true;
    }

    template<class T>
    bool CircularQueue<T>::deQueue(T &element) {
        if (isEmpty()) {
            return false;
        }
        //传入一个T的引用，方便接收队头，而不是返队头，这样函数返回布尔，调用完毕后，引用拿到队头
        element = m_pQueue[m_iHead];
        m_iLength--;
        m_iHead++;
        m_iHead = m_iHead % m_iCapacity;
        return true;
    }

    template<class T>
    bool CircularQueue<T>::isEmpty() const {
        return m_iLength == 0 ? true : false;
    }

    template<class T>
    bool CircularQueue<T>::isFull() const {
        return m_iCapacity == m_iLength ? true : false;
    }

    template<class T>
    int32_t CircularQueue<T>::length() const {
        return m_iLength;
    }

    template<class T>
    void CircularQueue<T>::printQueue(void(*pFunc)(T)) {
        for (int i = m_iHead; i < m_iHead + m_iLength; i++) {
            pFunc(m_pQueue[i % m_iCapacity]);
        }
    }

    template <class T>
    int32_t CircularQueue<T>::getData(int64_t version, std::vector<T> &elements) {
        int32_t  cnt{0};

        if(isEmpty()) return 0;

        /*for (int32_t i = m_iHead; i < m_iTail; i++) {
            std::cout << "head:" << m_iHead << "tail:" << m_iTail << "queue-version:" << m_pQueue[i].version() << std::endl;
        }*/
        if(version > m_pQueue[m_iTail-1].version()) {
           // std::cout << "version:" << version << "head:" << m_iHead << "tail:" << m_iTail << "queue-version:" << m_pQueue[m_iTail].version() << std::endl;
            return -1;
        }

        int32_t mid(0);
        int32_t from(m_iHead), to(m_iTail);

        while(from < to) {

            mid = (from + to) / 2;
            if (version == m_pQueue[mid].version()) {
                from = mid;
                break;
            } else {
                if (version > m_pQueue[mid].version()) {
                    from = mid;
                }
            }
        }

        for(int32_t i = from; i < to; i++) {
            cnt++;
            (m_pQueue + i)->setUpdateTime();
            elements.emplace_back(*(m_pQueue + i));
        }

        return cnt;

    }

}
}
}

