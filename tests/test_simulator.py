import unittest

from collector.simulator import build_ops, load_scenario


class SimulatorScriptTests(unittest.TestCase):
    def setUp(self):
        self.spec = load_scenario('fixtures/frames.json')

    def test_fault_mode_contains_all_event_classes(self):
        ops = build_ops(self.spec, 'faults')
        kinds = [op[0] for op in ops]
        self.assertIn('disconnect', kinds)
        labels = ' '.join(op[3] for op in ops if op[0] == 'send')
        self.assertIn('坏CRC帧', labels)
        self.assertIn('补发重放', labels)
        # split_at=[1,3,8] 把首帧拆成 4 段，只有最后一段期待 ACK。
        split_ops = [op for op in ops
                     if op[0] == 'send'
                     and any(op[3].endswith(f'分段{i}/4') for i in range(1, 5))]
        self.assertEqual(len(split_ops), 4)
        self.assertTrue(split_ops[-1][2])
        self.assertTrue(all(not op[2] for op in split_ops[:-1]))
        # 所有分段拼回来等于完整 24 字节帧。
        self.assertEqual(sum(len(op[1]) for op in split_ops), 24)

    def test_normal_mode_filters_faults(self):
        ops = build_ops(self.spec, 'normal')
        kinds = [op[0] for op in ops]
        self.assertNotIn('disconnect', kinds)
        labels = ' '.join(op[3] for op in ops if op[0] == 'send')
        self.assertNotIn('坏CRC帧', labels)
        # 正常模式仍保留重放（用于核对去重）与新代际帧。
        self.assertIn('补发重放', labels)
        self.assertIn('e2s1', labels)

    def test_coalesce_mode_merges_unsplit_frames(self):
        ops = build_ops(self.spec, 'coalesce')
        merged = [op for op in ops if op[0] == 'send' and '合并发送' in op[3]]
        self.assertTrue(merged)
        # 合并操作携带每一帧的身份，用于逐一核对 ACK。
        self.assertTrue(all(isinstance(op[4], list) and op[4] for op in merged))

    def test_unknown_event_type_rejected(self):
        spec = {'events': [{'type': 'mystery'}]}
        with self.assertRaises(ValueError):
            build_ops(spec, 'faults')


if __name__ == '__main__':
    unittest.main()
