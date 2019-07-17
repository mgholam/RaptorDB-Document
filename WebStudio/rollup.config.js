import svelte from 'rollup-plugin-svelte';
import resolve from 'rollup-plugin-node-resolve';
import commonjs from 'rollup-plugin-commonjs';
import livereload from 'rollup-plugin-livereload';
import { terser } from 'rollup-plugin-terser';
//import { uglify } from 'rollup-plugin-uglify';


const production = !process.env.ROLLUP_WATCH;
const dir = production ? "dist" : "public";

export default {
    input: production ? 'src/main.js' : 'src/debug.js',

    output: {
        sourcemap: !production,
        format: 'iife',
        name: 'app',
        file: dir + '/bundle.js'
    },
    plugins: [
        svelte({
            // enable run-time checks when not in production
            dev: !production,
            // we'll extract any component CSS out into
            // a separate file — better for performance
            css: css => {
                css.write(dir + '/bundle.css', !production); // disable sourcemap
            }
        }),

        // If you have external dependencies installed from
        // npm, you'll most likely need these plugins. In
        // some cases you'll need additional configuration —
        // consult the documentation for details:
        // https://github.com/rollup/rollup-plugin-commonjs
        resolve({ browser: true }),
        commonjs(),

        // Watch the `public` directory and refresh the
        // browser on changes when not in production
        !production && livereload(dir), // 'public'

        // If we're building for production (npm run build
        // instead of npm run dev), minify

        production && terser()
        //        production && uglify()
    ],
    watch: {
        clearScreen: true
    }
};