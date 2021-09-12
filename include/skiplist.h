#pragma once
/* ************************************************************************
> Description:   
    由于是模板类，因此声明和定义在同一个头文件
    假设原始链表有 n 个结点，那么索引的层级就是 log(n)-1(不包括原始链表)
    因为在每一层的访问次数是常量，因此查找结点的平均时间复杂度是O（logn）
    比起常规的查找方式，也就是线性依次访问链表节点的方式，效率要高得多
    基于链表的优化增加了额外的空间开销
    假设原始链表有 n 个结点，那么各层索引的结点总数是 n/2 + n/4 + n/8 + n/16 + ... + 2，约等于n。
    因此优化之后的数据结构所占空间，是原来的2倍。这是典型的以空间换时间的做法
 ************************************************************************/
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <chrono>
#include <ctime>
#include <stdlib.h>
#include <list>
#include <unistd.h>

#define STORE_FILE "./store/dumpFile.txt"
#define AOF_FILE "./store/append_on_file.txt"
// sigalrm 定时信号，加入待删除链表
// 查询时遇到直接删
// 待删除链表啥时候启动？


std::mutex mtx; // mutex for critical section
std::string delimiter = ":";
typedef std::chrono::seconds SECONDS;
typedef std::chrono::high_resolution_clock Clock;
typedef Clock::time_point TimeStamp;

// Class template to implement node
// 跳表节点模板
template <typename K, typename V>
class Node {
public:
    // 节点定义
    Node() {}
    Node(K k, V v, int, int);
    ~Node();

    // 节点操作
    K get_key() const;
    V get_value() const;
    void set_value(V);

    // 超时设置
    bool is_set_expire() const {
        return this->set_expire;
    }
    bool is_expired() const;
    time_t get_expire_systime() const;
    TimeStamp get_expire_timestamp() const;
    void set_expire_time(int);
    void expand_expire_time(int);

    // 输出当前节点情况
    void print() const;

    // Linear array to hold pointers to next node of different level
    // 指向所连接的下一个节点，指针数组
    Node<K, V> **forward;       // 类似
    // Node<K, V> *forward[node_level];

    // 节点等级
    int node_level;
private:
    K key;
    V value;
    TimeStamp expire_time;
    bool set_expire;
};

// 根据 timeout 设置超时时间
template <typename K, typename V>
Node<K, V>::Node(const K k, const V v, int level, int timeout) {
    this->key = k;
    this->value = v;
    this->node_level = level;
    if (timeout > 0) {
        this->expire_time = Clock::now() + SECONDS(timeout);
        this->set_expire = true;
    }
    else {
        this->expire_time = Clock::now();
        this->set_expire = false;
    }

    // level + 1, because array index is from 0 - level
    this->forward = new Node<K, V>* [level + 1];

    // Fill forward array with 0(NULL)
    memset(this->forward, 0, sizeof(Node<K, V>*) * (level + 1));
};


template <typename K, typename V>
Node<K, V>::~Node() {
    delete[] forward;
};

template <typename K, typename V>
K Node<K, V>::get_key() const {
    return key;
};

template <typename K, typename V>
V Node<K, V>::get_value() const {
    return value;
};

template <typename K, typename V>
bool Node<K, V>::is_expired() const{
    if (this->set_expire) {
        if (std::chrono::duration_cast<SECONDS>(this->get_expire_timestamp() - Clock::now()).count() <= 0) 
        {
            return true; 
        }
    }
    return false;
}

template <typename K, typename V>
void Node<K, V>::set_value(V value) {
    this->value = value;
};

// 获取超时时间
template <typename K, typename V>
time_t Node<K, V>::get_expire_systime() const{
    time_t time = std::chrono::system_clock::to_time_t(this->expire_time);
    return time;
}

template <typename K, typename V>
TimeStamp Node<K, V>::get_expire_timestamp() const{
    return this->expire_time;
}

// 添加超时时间设置
template <typename K, typename V>
void Node<K, V>::set_expire_time(int timeout) {
    this->set_expire = true;
    this->expire_time += SECONDS(timeout);
}

// 延长超时时间
template <typename K, typename V>
void Node<K, V>::expand_expire_time(int timeout) {
    if (this->set_expire) {
        this->expire_time += SECONDS(timeout);
    }
    return;
}


// 打印节点情况
template <typename K, typename V>
void Node<K, V>::print() const {
    std::string outputs("The Node is ");
    outputs += this->get_key() + " : " + this->get_value();
    if (this->is_set_expire()) {
        char buf[50] = "-> expire_time : ";
        time_t expire_time = this->get_expire_systime();
        strftime(buf + strlen(buf), sizeof(buf), "%Y-%m-%d %H:%M:%S", localtime(&expire_time));
        outputs += buf;
    }
    std::cout << outputs << std::endl;
}

// Class template for Skip list
// 跳表类模板
template <typename K, typename V>
class SkipList {
public:
    SkipList(int, int);
    ~SkipList();

    int get_random_level();

    Node<K, V> *create_node(K, V, int, bool);
    int insert_element(K, V);

    void display_list();

    // 节点操作
    bool is_exist_element(K);
    bool search_element(K);
    void delete_element(K);
    Node<K, V>* set_expire_key(K, int);

    // 加载文件
    bool dump_file();
    void load_file();
    int size();

private:
    // 通过字符串读取节点值
    void get_key_value_from_string(const std::string &str, std::string *key, std::string *value);
    bool is_valid_string(const std::string &str);
    // 默认超时时间
    int timeout;
    // std::list<Node*> wait_to_delete;

private:
    // Maximum level of the skip list
    // 最大 level 限制
    int _max_level;

    // current level of skip list
    // 当前的 level
    int _skip_list_level;

    // pointer to header node
    Node<K, V>* _header;

    // file operator，用于读取和保存数据库
    std::ofstream _file_writer;
    std::ifstream _file_reader;

    // skiplist current element count
    // 跳表元素数量
    int _element_count;
};

// 创建新节点
template <typename K, typename V>
Node<K, V>* SkipList<K, V>::create_node(const K k, const V v, int level, bool set_expire) {
    int timeout = -1;
    if (set_expire) {
        timeout = this->timeout + rand() % 1800;
    }
    Node<K, V> *n = new Node<K, V>(k, v, level, timeout);
    return n;
}

// Insert given key and value in skip list
// return 1 means element exists
// return 0 means insert successfully
/* 
                           +------------+
                           |  insert 50 |
                           +------------+
level 4     +-->1+                                                        100
                 |
                 |                        insert +----+
level 3         1+---------->10+---------------> | 50 |          70       100
                                                 |    |
                                                 |    |
level 2         1            10         30       | 50 |          70       100
                                                 |    |
                                                 |    |
level 1         1    4       10         30       | 50 |          70       100
                                                 |    |
                                                 |    |
level 0         1    4    9  10         30   40  | 50 |  60      70       100
                                                 +----+

*/
template <typename K, typename V>
int SkipList<K, V>::insert_element(const K key, const V value) {

    mtx.lock();
    Node<K, V>* current = this->_header;        // 获得头节点

    // create update array and initialize it
    // update is array which put node that the node->forward[i] should be operated later
    // 用于储存将要操作的节点指针数组，由于每级至多添加一个节点指针，因此最大为 _max_level + 1
    Node<K, V>* update[_max_level + 1];
    memset(update, 0, sizeof(Node<K, V>*) * (_max_level + 1));

    // start form highest level of skip list
    // 从最高级节点开始寻找
    for (int i = _skip_list_level; i >= 0; i--) {
        // 不断遍历直到下一节点为空或 key 值大于插入节点的 key 值
        // 此时将在最高级节点停止查找，并从最高级节点的下一级节点
        while (current->forward[i] != NULL && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    // reached level 0 and forward pointer to right node, which is desired to insert key.
    current = current->forward[0];

    // if current node have key equal to searched key, we get it
    // 成功找到节点
    if (current != NULL && current->get_key() == key) {
        std::cout << "The node is exists, but has been changed" << std::endl;
        mtx.unlock();
        return 1;
    }

    // if current is NULL that means we have reached to end of the level
    // if current's key is not equal to key that means we have to insert node between update[0] and current node
    if (current == NULL || current->get_key() != key) {

        // Generate a random level for node
        int random_level = get_random_level();

        // If random level is greater thar skip list's current level, initialize update value with pointer to header
        if (random_level > _skip_list_level) {
            for (int i = _skip_list_level + 1; i < random_level + 1; i++)
            {
                update[i] = _header;
            }
            _skip_list_level = random_level;
        }

        // create new node with random level generated
        Node<K, V> *inserted_node = create_node(key, value, random_level, false);

        // insert node
        for (int i = 0; i <= random_level; i++)
        {
            // update 节点数组记录了所有的 current 节点，这些节点自身节点值都小于插入节点
            // 但这些节点的下一节点值都大于插入节点
            inserted_node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = inserted_node;
        }
        std::cout << "Successfully inserted key" << std::endl;
        _element_count++;
    }
    mtx.unlock();
    return 0;
}

// Display skip list
template <typename K, typename V>
void SkipList<K, V>::display_list()
{

    std::cout << "\n*****Skip List*****"
              << "\n";
    for (int i = 0; i <= _skip_list_level; i++)
    {
        Node<K, V> *node = this->_header->forward[i];
        std::cout << "Level " << i << ": ";
        while (node != NULL)
        {
            node->print();
            node = node->forward[i];        // 指向同一级节点
            // forward[0] 即为原始链表
        }
        std::cout << std::endl;
    }
}

// Dump data in memory to file
// 只保存原始链表节点
template <typename K, typename V>
bool SkipList<K, V>::dump_file()
{

    std::cout << "dump_file-----------------" << std::endl;
    _file_writer.open(STORE_FILE);

    if (_file_writer) {
        Node<K, V> *node = this->_header->forward[0];

        while (node != NULL)
        {
            // 节点未超时
            if (!node->is_expired())  {
                _file_writer << node->get_key() << " : " << node->get_value() << "\n";
            }
            node->print();
            node = node->forward[0];
        }

        _file_writer.flush();
        _file_writer.close();
        return true;
    }
    else {
        char* path;
        std::cout << "The file open failed\n";
        path = get_current_dir_name();
        std::cout << "The work path is " << path << std::endl;
        return false;
    }
}

// Load data from disk
template <typename K, typename V>
void SkipList<K, V>::load_file()
{

    _file_reader.open(STORE_FILE);
    std::cout << "load_file-----------------" << std::endl;
    std::string line;
    std::string *key = new std::string();
    std::string *value = new std::string();
    while (getline(_file_reader, line))
    {
        get_key_value_from_string(line, key, value);
        if (key->empty() || value->empty())
        {
            continue;
        }
        insert_element(*key, *value);
        std::cout << "key: " << *key << " value: " << *value << std::endl;
    }
    _file_reader.close();
}

// Get current SkipList size
template <typename K, typename V>
int SkipList<K, V>::size()
{
    return _element_count;
}

template <typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string &str, std::string *key, std::string *value)
{

    if (!is_valid_string(str))
    {
        return;
    }
    *key = str.substr(0, str.find(delimiter));
    *value = str.substr(str.find(delimiter) + 1, str.length());
}

template <typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string &str)
{

    if (str.empty())
    {
        return false;
    }
    if (str.find(delimiter) == std::string::npos)
    {
        return false;
    }
    return true;
}

// Delete element from skip list
template <typename K, typename V>
void SkipList<K, V>::delete_element(K key)
{
    mtx.lock();
    Node<K, V> *current = this->_header;
    Node<K, V> *update[_max_level + 1];
    memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1));

    // start from highest level of skip list
    for (int i = _skip_list_level; i >= 0; i--)
    {
        while (current->forward[i] != NULL && current->forward[i]->get_key() < key)
        {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];
    if (current != NULL && current->get_key() == key)
    {

        // start for lowest level and delete the current node of each level
        for (int i = 0; i <= _skip_list_level; i++)
        {

            // if at level i, next node is not target node, break the loop.
            if (update[i]->forward[i] != current)
                break;
            // updata[i] -> current
            update[i]->forward[i] = current->forward[i];
        }

        // Remove levels which have no elements
        // 检查高级指针是否被删除
        while (_skip_list_level > 0 && _header->forward[_skip_list_level] == 0)
        {
            _skip_list_level--;
        }

        std::cout << "Successfully deleted key " << key << std::endl;
        _element_count--;
    }
    mtx.unlock();
    return;
}

// Search for element in skip list
/*
                           +------------+
                           |  select 60 |
                           +------------+
level 4     +-->1+                                                      100
                 |
                 |
level 3         1+-------->10+------------------>50+           70       100
                                                   |
                                                   |
level 2         1          10         30         50|           70       100
                                                   |
                                                   |
level 1         1    4     10         30         50|           70       100
                                                   |
                                                   |
level 0         1    4   9 10         30   40    50+-->60      70       100
*/
template <typename K, typename V>
bool SkipList<K, V>::search_element(K key)
{
    Node<K, V> *current = _header;

    // start from highest level of skip list
    for (int i = _skip_list_level; i >= 0; i--)
    {
        while (current->forward[i] && current->forward[i]->get_key() < key)
        {
            current = current->forward[i];
        }
    }

    //reached level 0 and advance pointer to right node, which we search
    current = current->forward[0];

    // if current node have key equal to searched key, we get it
    if (current != NULL and current->get_key() == key)
    {
        current->print();
        return true;
    }

    std::cout << "Not Found Key:" << key << std::endl;
    return false;
}

// 查看键是否存在
template <typename K, typename V>
bool SkipList<K, V>::is_exist_element(K key) 
{
    Node<K, V> *current = _header;

    // start from highest level of skip list
    for (int i = _skip_list_level; i >= 0; i--)
    {
        while (current->forward[i] && current->forward[i]->get_key() < key)
        {
            current = current->forward[i];
        }
    }

    //reached level 0 and advance pointer to right node, which we search
    current = current->forward[0];

    // if current node have key equal to searched key, we get it
    if (current != NULL and current->get_key() == key)
    {
        return true;
    }
    return false;
}

// 设置超时
template <typename K, typename V>
Node<K, V>* SkipList<K, V>::set_expire_key(K key, int time) {
    Node<K, V> *current = _header;

    // start from highest level of skip list
    for (int i = _skip_list_level; i >= 0; i--)
    {
        while (current->forward[i] && current->forward[i]->get_key() < key)
        {
            current = current->forward[i];
        }
    }

    current = current->forward[0];

    if (current != NULL and current->get_key() == key)
    {
        current->set_expire_time(this->timeout);
        current->expand_expire_time(time);
        current->print();
        return current;
    }

    std::cout << "Not Found Key:" << key << std::endl;
    return nullptr;
}

// construct skip list
template <typename K, typename V>
SkipList<K, V>::SkipList(int max_level, int timeout_)
{

    this->_max_level = max_level;
    this->_skip_list_level = 0;
    this->_element_count = 0;
    this->timeout = timeout_;       // 设置默认超时时间
    // create header node and initialize key and value to null
    K k;
    V v;
    this->_header = new Node<K, V>(k, v, _max_level, -1);
};

template <typename K, typename V>
SkipList<K, V>::~SkipList()
{

    if (_file_writer.is_open())
    {
        _file_writer.close();
    }
    if (_file_reader.is_open())
    {
        _file_reader.close();
    }
    delete _header;
}

template <typename K, typename V>
int SkipList<K, V>::get_random_level()
{

    int k = 1;
    while (rand() % 2)
    {
        k++;
    }
    k = (k < _max_level) ? k : _max_level;
    return k;
};
