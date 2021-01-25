package server;

import java.util.HashMap;

public class LRUCache<K, V> implements Cache<K, V> {

    private static class Node<K, V> {
        public K key;
        public V value;
        public Node<K, V> prev;
        public Node<K, V> next;

        public Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    private Node<K, V> head = new Node<>(null, null);
    private Node<K, V> tail = new Node<>(null, null);
    private HashMap<K, Node> dic = new HashMap<>();
    private int capacity;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        head.next = tail;
        tail.prev = head;
    }

    @Override
    public int getSize() {
        return dic.size();
    }

    @Override
    public int getCapacity() {
        return capacity;
    }

    @Override
    public V get(K key) {
        if (dic.containsKey(key)) {
            Node<K, V> node = dic.get(key);
            removeNode(node);
            addNode(node);
            return node.value;
        }
        return null; //TODO: what if value is null?
    }

    @Override
    public void put(K key, V value) {
        if (dic.containsKey(key)) {
            this.removeNode(dic.get(key));
        }
        Node<K, V> node = new Node<>(key, value);
        addNode(node);
        dic.put(key, node);
        if (getSize() > capacity) {
            key = head.next.key;
            removeNode(head.next);
            dic.remove(key);
        }
    }

    private void removeNode(Node node) {
        Node prevNode = node.prev;
        Node nextNode = node.next;
        prevNode.next = nextNode;
        nextNode.prev = prevNode;
    }

    private void addNode(Node node) {
        Node prevNode = tail.prev;
        prevNode.next = node;
        node.prev = prevNode;
        tail.prev = node;
        node.next = tail;
    }

    @Override
    public void clear() {
        this.dic.clear();
        this.head = new Node<>(null, null);
        this.tail = new Node<>(null, null);
        head.next = tail;
        tail.prev = head;
    }
}
