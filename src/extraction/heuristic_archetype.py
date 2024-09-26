from itertools import groupby


class HeuristicArchetype:
    def __init__(self, layouts):
        self.omitted_layouts = []
        groups = {}
        for k, g in groupby(layouts, key=lambda x: x['class']):
            if k in ['page-header', 'page-footer']:
                self.omitted_layouts += list(g)
            elif k in groups:
                groups[k] += list(g)
            else:
                groups[k] = list(g)

        self.footnote = groups.pop('footnote', [])
        self.layouts = sorted([item for sublist in groups.values() for item in sublist], key=lambda l: (l['y'], l['x']))
        self.layouts += self.footnote

    def __iter__(self):
        return iter(self.layouts)
