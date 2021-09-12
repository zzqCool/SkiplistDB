#include <list>
#include <unordered_map>
#include <iostream>
#include <mutex>
using namespace std;

std::mutex lru_mtx;

// 页面节点
// K : K,   V : PAGE*
template<typename K, typename V>
struct LinkNode {
    K key;
    V value;
    LinkNode* pre;
    LinkNode* next;

    LinkNode() = default;
    LinkNode(K _key, V _value) : key(_key), value(_value), pre(nullptr), next(nullptr) {     }
    ~LinkNode() {
        cout << "析构中... " << key << "->" << value << endl;
    }

    bool is_expired() {
        return value->is_expired();
    }
};

template<typename K, typename V>
class LRU_Cache {
    typedef LinkNode<K, V> Node;
public:
    // 构造函数
    LRU_Cache(int cap) : capacity(cap) {
        head = new Node();
        tail = new Node();
        head->pre = nullptr;
        tail->next = nullptr;

        head->next = tail;
        tail->pre = head;
    }

    // 析构函数
    ~LRU_Cache() {
        if (head != nullptr) {
            delete head;
            head = nullptr;
        }
        if (tail != nullptr) {
            delete tail;
            tail = nullptr;
        }
        for (auto& a : cache) {
            if (a.second != nullptr) {
                delete a.second;
                a.second = nullptr;
            }
        }
    }

    // 更新节点值
    void set_key(const K _key, const V _val) {
        lru_mtx.lock();
        // 页面已存在
        if (cache.find(_key) != cache.end()) {
            Node* node = cache[_key];
            remove_node(node);
            node->value = _val;
            push_node(node);
            // lru_mtx.unlock();
            return;
        }
        // 页面已满
        if (cache.size() == this->capacity) {
            K top_key = head->next->key;      // 取得 k 值，方便删除
            remove_node(head->next);            // 移除头部节点的下一个
            Node* node = cache[top_key];    // 删除节点，否则会导致内存泄漏
            delete node;
            node = nullptr;
            cache.erase(top_key);               // 在 cache 中删除该节点
        }
        Node* node = new Node(_key, _val);
        push_node(node);            // 加至尾部节点
        cache[_key] = node;         // 添加至 cache 中
        lru_mtx.unlock();
    }

    // 删除节点
    void delete_key(const K key) {
        lru_mtx.lock();
        Node* node = cache[key];
        remove_node(node);
        delete node;
        node = nullptr;
        cache.erase(key);               // 在 cache 中删除该节点
        lru_mtx.unlock();
    }

    // 取出页面中存储的值并更新链表
    V get_value(const K _key) {
        lru_mtx.lock();
        if (cache.find(_key) != cache.end()) {
            Node* node = cache[_key];
            remove_node(node);
            push_node(node);
            // lru_mtx.unlock();
            return node->value;
        }
        lru_mtx.unlock();
        return nullptr;
    }

    // 获得指向头指针的键
    K get_head_key() const{
        return head->next->key;
    }

    // 依次打印双向链表节点值
    void print() const{
        Node* node = this->head->next;
        while (node != this->tail->pre) {
            cout << node->value->get_value() << "->";
            node = node->next;
        }
        cout << node->value->get_value() << endl;
    }

    // 检查键是否存在过期字典
    bool is_exist(K key) {
        auto it = cache.find(key);
        return it != cache.end();
    }

    // 检查键是否过期
    bool is_expired(K key) {
        if (is_exist(key)) {
            return cache[key]->is_expired();
        }
        return false;
    }

    int get_current_capacity() {
        return cache.size();
    }
private:
    // 删除节点
    void remove_node(Node* node) {
        node->pre->next = node->next;
        node->next->pre = node->pre;
    }

    // 将节点加入双向链表末尾
    void push_node(Node* node) {
        tail->pre->next = node;
        node->pre = tail->pre;
        node->next = tail;
        tail->pre = node;
    }


private:
    unordered_map<K, Node*> cache;
    int capacity;           // 缓存容量
    Node* head;
    Node* tail;
};


