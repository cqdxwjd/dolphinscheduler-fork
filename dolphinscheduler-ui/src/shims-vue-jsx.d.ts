// src/shims-vue-jsx.d.ts
import {JSX} from '@vue/runtime-dom'

declare global {
    namespace JSX {
        // 扩展 JSX  intrinsic 元素类型（如 div, span 等）
        interface IntrinsicElements {
            [key: string]: any
        }
    }
}