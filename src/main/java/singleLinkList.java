public class singleLinkList {
    //创建头结点
    LinkNode head = new LinkNode(2, "张三");

    //增加节点
    public void add(LinkNode newnode) {
        //创建临时变量替换head
        LinkNode temp = head;

        //循环遍历找到链表的末尾
        while (true) {
            if (temp.next == null) {
                break;
            }
            temp = temp.next;
        }
        temp.next = newnode;
    }

    //添加节点的第二种方法 //按顺序存取
    public void add2(LinkNode node) {
        //将头结点 赋值给temp
        LinkNode temp = head;
        //定义一个flag来 管理状态
        boolean flag = false;
        //
        while (true) {
            if (temp.next == null) {
                break;
            }
            if (temp.next.no > node.no) {
                break;
            } else if (temp.next.no == node.no) {
                flag = true;
                break;
            }
            temp = temp.next;

        }
        if (flag) {
            System.out.println("节点" + node.no + "已经存在");
        } else {
            node.next = temp.next;
            temp.next = node;
        }

    }

    //链表的修改
    public void update(LinkNode node) {
        if (head.next == null) {
            System.out.println("这是一个空链表");
            return;
        }
        LinkNode temp = head.next;
        boolean flag = false;

        while (true) {
            if (temp == null) {
                break;
            }
            if (temp.no == node.no) {
                flag = true;
                break;
            }
            temp = temp.next;

        }
        if (flag) {
            temp.name = node.name;
        } else {
            System.out.println("没有找到该节点");
        }

    }

    //链表的删除
    public void del(int no) {
        LinkNode temp = head;
        boolean flag = false;

        while (true) {
            if (temp.next == null) {
                break;
            }
            if (temp.next.no == no) {
                flag = true;
                break;
            }
            temp = temp.next;
        }
        if (flag) {
            temp.next = temp.next.next;
        } else {
            System.out.println("没有该节点");
        }
    }

    //链表的遍历
    public void list() {
        if (head.next == null) {
            System.out.println("此链表为空链表");
            return;
        }
        LinkNode temp = head.next;
        while (true) {

            if (temp == null) {
                break;
            }
            System.out.println(temp);
            temp = temp.next;
        }

    }
}

//创建节点
class LinkNode {
    public int no;
    public String name;
    public LinkNode next; //指向下一个节点

    public LinkNode(int no, String name) {
        this.no = no;
        this.name = name;
    }


    @Override
    public String toString() {
        return "no: " + no + "  " + "name: " + name;
    }
}

class Demo {
    public static void main(String[] args) {
        LinkNode node1 = new LinkNode(4, "张飞");
        LinkNode node2 = new LinkNode(2, "关羽");
        LinkNode node3 = new LinkNode(3, "刘备");
        LinkNode node4 = new LinkNode(5, "赵云");
        singleLinkList singleLinkList = new singleLinkList();
        singleLinkList.add2(node1);
        singleLinkList.add2(node2);
        singleLinkList.add2(node3);
        singleLinkList.add2(node4);
        singleLinkList.list();
        System.out.println("******************************");
        System.out.println(getLength(singleLinkList.head));
        System.out.println(findIndexNode(singleLinkList.head, 1));
//        singleLinkList.update(node4);
//        singleLinkList.del(1);
//        singleLinkList.list();

    }

    /**
     * @param head 链表的头结点
     * @return 返回链表的长度
     */
    //单链表面试题  带头节点不统计头结点
    public static int getLength(LinkNode head) {
        int length = 0;
        if (head.next == null) {
            return 0;
        }
        LinkNode temp = head.next;
        while (true) {
            if (temp == null) {
                break;
            }
            length++;
            temp = temp.next;
        }
        return length;
    }

    /**
     * @param head  头结点
     * @param index 倒数值
     * @return
     */
    //统计倒数第一个节点
    public static LinkNode findIndexNode(LinkNode head, int index) {
        int size = getLength(head);

        int nodenum = size - index;
        LinkNode temp = head;   //指针应该从头结点开始位移  nodenum代表的是指针移动几下 从head。next移动会多移一个

        if (nodenum > size && nodenum < 0) {
            return null;
        } else {
            for (int i = 0; i < nodenum; i++) {

                temp = temp.next;
            }
        }
        return temp;
    }
}
